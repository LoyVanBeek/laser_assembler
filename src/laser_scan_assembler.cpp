/*********************************************************************
* Software License Agreement (BSD License)
*
*  Copyright (c) 2008, Willow Garage, Inc.
*  All rights reserved.
*
*  Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions
*  are met:
*
*   * Redistributions of source code must retain the above copyright
*     notice, this list of conditions and the following disclaimer.
*   * Redistributions in binary form must reproduce the above
*     copyright notice, this list of conditions and the following
*     disclaimer in the documentation and/or other materials provided
*     with the distribution.
*   * Neither the name of the Willow Garage nor the names of its
*     contributors may be used to endorse or promote products derived
*     from this software without specific prior written permission.
*
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
*  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
*  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
*  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
*  COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
*  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
*  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
*  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
*  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
*  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
*  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
*  POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/


#include "laser_geometry/laser_geometry.h"
#include "sensor_msgs/image_encodings.h"
#include "sensor_msgs/Image.h"
#include "sensor_msgs/LaserScan.h"
#include "laser_assembler/base_assembler.h"
#include "laser_assembler/AssembleScans2.h"
#include "laser_assembler/StartCollection.h"
#include "laser_assembler/StopCollectionAndAssembleScans2.h"
#include "filters/filter_chain.h"
#include "opencv2/imgproc/imgproc.hpp"
#include "cv_bridge/cv_bridge.h"
#include "tf/transform_datatypes.h"
#include <limits>
#include <vector>

using namespace laser_geometry;
using namespace std ;

namespace laser_assembler
{

/**
 * \brief Maintains a history of laser scans and generates a point cloud upon request
 */
class LaserScanAssembler : public BaseAssembler<sensor_msgs::LaserScan>
{
public:
  LaserScanAssembler() : BaseAssembler<sensor_msgs::LaserScan>("max_scans"), filter_chain_("sensor_msgs::LaserScan")
  {
    ROS_INFO("Constructor of LaserScanAssembler");
    // ***** Set Laser Projection Method *****
    private_ns_.param("ignore_laser_skew", ignore_laser_skew_, true);

    // Automatically subscribe to scans or wait for service?
    private_ns_.param("subscribe_directly", subscribe_directly_, true);

    // configure the filter chain from the parameter server
    filter_chain_.configure("filters", private_ns_);

    start_collection_server_ = n_.advertiseService("start_collection", &LaserScanAssembler::startCollection, this);
    stop_collection_server_ = n_.advertiseService("stop_collection_and_assemble_scans2", &LaserScanAssembler::stopCollectionAndAssembleScans2, this);
    
    pointcloud2_pub_ = n_.advertise<sensor_msgs::PointCloud2>("depth", 1);
    stretched_range_image_pub_ =  n_.advertise<sensor_msgs::Image> ("range_image", 1);
    stretched_depth_image_pub_ =  n_.advertise<sensor_msgs::Image> ("depth_image", 1);

    if (subscribe_directly_)
      subscribe();

  }

  ~LaserScanAssembler()
  {

  }

  void subscribe()
  {
    // Have different callbacks, depending on whether or not we want to ignore laser skews.
    if (ignore_laser_skew_)
    {
      start("scan");
    }
    else
    {
      start();
      skew_scan_sub_ = n_.subscribe("scan", 10, &LaserScanAssembler::scanCallback, this);
    }
    ROS_INFO("Started listening to scans");
  }

  void unsubscribe()
  {
    ROS_INFO("Stopping to listen to scans");
    if (ignore_laser_skew_)
    {
      scan_sub_.unsubscribe();
    }
    else
    {
      skew_scan_sub_.shutdown();
    }
    ROS_INFO("Stopped listening to scans");
  }

  cv::Mat sortMatRowBy(cv::Mat key, cv::Mat unsorted, std::vector<double>& sorted_keys, std::multimap<double, std::size_t>& reordering)
  {
    ROS_DEBUG_STREAM("key.size(): " << key.size() << ", unsorted.size(): " << unsorted.size());
    cv::Mat sorted = cv::Mat(unsorted.size(), unsorted.type());

    // A multimap inserts elements in order (per https://stackoverflow.com/questions/1577475/c-sorting-and-keeping-track-of-indexes)
    std::multimap<double, std::size_t> remapping;
    for (uint origIndex = 0; origIndex != key.size[0]; ++origIndex)
    {
        reordering.insert({key.at<double>(origIndex), origIndex});
    }

    sorted_keys = std::vector<double>();
    for (auto it=reordering.begin(); it!=reordering.end(); ++it)
    {
      auto sortedIndex = std::distance(reordering.begin(), it);
      // ROS_DEBUG_STREAM("origIndex: " << (*it).second << ", height: " << (*it).first << ", sortedIndex = " << sortedIndex);
      unsorted.row((*it).second).copyTo(sorted.row(sortedIndex));
      sorted_keys.push_back((*it).first);
    }

    return sorted;
  }

  cv::Mat reorderImageRows(cv::Mat unsorted, std::multimap<double, std::size_t> reordering)
  {
    cv::Mat sorted = cv::Mat(unsorted.size(), unsorted.type());
    for (auto it=reordering.begin(); it!=reordering.end(); ++it)
    {
      auto sortedIndex = std::distance(reordering.begin(), it);
      ROS_DEBUG_STREAM("origIndex: " << (*it).second << ", height: " << (*it).first << ", sortedIndex = " << sortedIndex);
      unsorted.row((*it).second).copyTo(sorted.row(sortedIndex));
    }
    return sorted;
  }

  /**
   * @brief Given a sorted array of numbers and a number, return an imaginary index at which the query number would occur
   *
   * This assumes the query is in the range of numbers covered by the sorted_array.
   * The interpolation is simply linear between the first number smaller than the query and the first number larger than the query
   * 
   * For example, let sorted_array be {0, 2, 4, 5} and query = 3.0.
   * 3 sits exactly halfway between sorted_array[1] (=2) and sorted_array[2] (=4)
   * Then the interpolated index would be 1.5.
   * For query = 2.2, the index would be 1.1.
   * For query = 4.3, the index would be 2.3, etc.
   *
   * @param sorted_array the array in which to for where the query number would go
   * @param query number for which to find an index for
   * @return double
   */
  double findInterpolatedIndex(std::vector<double> sorted_array, double query)
  {
    //TODO: Handle cases where the number is outside the range
    // ROS_INFO_STREAM("Looking for " << query);

    // From https://alexsm.com/cpp-closest-lower-bound/
    auto iter_geq = std::lower_bound(
        sorted_array.begin(),
        sorted_array.end(),
        query
    );

    if (iter_geq == sorted_array.begin())
    {
        return 0;
    }

    double before = *(iter_geq - 1);
    double after = *(iter_geq);

    size_t index_under = 0;
    double value_under = 0;
    size_t index_above = 0;
    double value_above = 0;

    if (fabs(query - before) < fabs(query - after))
    {
      index_under = iter_geq - sorted_array.begin() - 1;
      value_under = before;
      index_above = iter_geq - sorted_array.begin();
      value_above = after;
    }
    else
    {
      index_under = iter_geq - sorted_array.begin();
      value_under = before;
      index_above = iter_geq - sorted_array.begin()+1;
      value_above = after;
    }

    double interpolated_index = index_under + ((query - value_under) / (value_above - value_under));
        ROS_INFO_STREAM(
      "query: " << query <<
      ", index_under: " << index_under <<
      ", value_under: " << value_under <<
      ", index_above: " << index_above <<
      ", value_above: " << value_above <<
      ", interpolated_index: " << interpolated_index);
    return interpolated_index;
  }

  unsigned int GetPointsInScan(const sensor_msgs::LaserScan& scan)
  {
    return (scan.ranges.size ());
  }

  void ScanToImages(const string& fixed_frame_id, const sensor_msgs::LaserScan& scan_in)
  {
    /* How to project the range data to a plane perpendicular to the optical axis of the scanner and through the scanner's center?
     * For each range measurement:
     * - X distance over the plane is then $range * sin(angle)$
     * - Depth to the plane is then $range * cos(angle)$
     *
     * The image X dimension is dependent on max range & FoV max angle, since $X_max = range_max * cos(angle_max)$
     *
     */
    auto depth_min_x = current_req_.max_range * sin(current_req_.min_width);  // [m] At what X would the max range fall at the first ray in the FoV?
    auto depth_max_x = current_req_.max_range * sin(current_req_.max_width);  // [m] At what X would the max range fall at the last ray in the FoV?
    // ROS_INFO_STREAM("current_req_.max_range: " << current_req_.max_range << ", current_req_.min_width: " << current_req_.min_width << ", sin(current_req_.min_width): " << sin(current_req_.min_width));
    auto depth_width_x = abs(depth_max_x - depth_min_x);  // [m] Width of the image in meters
    auto depth_max_val = current_req_.max_range;  // [m]Maximum depth represented in the depth image

    auto vertical_step = abs(current_req_.max_height - current_req_.min_height) / current_req_.vertical_resolution;  // [m] Height of a pixel in both depth and range image
    auto range_horizontal_step = abs(current_req_.max_width - current_req_.min_width) / current_req_.horizontal_resolution;  // [rad] angle covered by a pixel in the range image
    auto depth_horizontal_step = depth_width_x / current_req_.horizontal_resolution;  // [m] Width of a pixel in the depth image

    tf::StampedTransform transform;
    tf_->lookupTransform(fixed_frame_id, scan_in.header.frame_id, ros::Time(0), transform);
    auto height = (double)transform.getOrigin().z(); // TODO: Make this bit configurable to be generic
    uint row = stretched_range_mat_.rows - ((height - current_req_.min_height) / vertical_step);  // TODO: check proper types, absolutes and rounding
    // ROS_INFO_STREAM("height: " << height << ", max_height: " << current_req_.max_height << ", min_height: " << current_req_.min_height << ", vertical_step: " << vertical_step << ", row: " << row);
    // ROS_INFO_STREAM("max_width: " << current_req_.max_width << ", min_width: " << current_req_.min_width << ", horizontal_step: " << horizontal_step);
    // ROS_INFO_STREAM("scan_in.range_max: " << scan_in.range_max << ", scan_in.range_min: " << scan_in.range_min << ", std::numeric_limits<short>::max(): " << std::numeric_limits<uint16_t>::max());
    // ROS_INFO_STREAM("depth_min_x: " << depth_min_x << ", depth_max_x: " << depth_max_x << ", depth_width_x: " << depth_width_x << ", depth_max_val: " << depth_max_val << ", depth_horizontal_step: " << depth_horizontal_step);

    double previous_range = 0;
    uint previous_range_column = -1;

    // for (size_t i = 0; i < scan_in.ranges.size(); i++)
    // {
    //   auto range = scan_in.ranges[i];
    //   auto measurement_angle = scan_in.angle_min + i*scan_in.angle_increment;

    //   auto depth_x_distance = range * sin(measurement_angle);

    //   uint range_column = (measurement_angle - current_req_.min_width) / range_horizontal_step;
    //   uint depth_column = (depth_x_distance - depth_min_x) / depth_horizontal_step;
    //   // ROS_DEBUG_STREAM("column: " << column << ", index: " << i << ", measurement_angle: " << measurement_angle << ", max_width: " << current_req_.max_width << ", min_width: " << current_req_.min_width << ", horizontal_step: " << horizontal_step);
    //   // ROS_INFO_STREAM("i: " << i << ", row: " << row << ", range_column: " << range_column << ", range: " << range << ", depth_column: " << depth_column);
    //   if (range_column >= 0 && range_column < stretched_range_mat_.cols && row >= 0 && row < stretched_range_mat_.rows)
    //   {
    //     if (range < current_req_.min_range)
    //     {
    //       stretched_range_mat_.at<uint16_t>(row, range_column) = 0;
    //     }
    //     else
    //     {
    //       auto range_value = range * 1000;  // +1 value means 1mm further away
    //       // ROS_DEBUG_STREAM("scan_in.ranges["<<i<<"]: " << range << ", value: " << value << ", (uint16_t)value: " << (uint16_t)value);

    //       // If a pixel already has a value (which is always the case, since pixels are initialized to max range),
    //       // take the minimum value of the new and current
    //       stretched_range_mat_.at<uint16_t>(row, range_column) = (uint16_t)range_value;
    //     }
    //   }
    //   else
    //   {
    //     ROS_DEBUG("Row and/or column outside of range image");
    //   }
    //   previous_range = range;
    //   previous_range_column = range_column;


    //   if (depth_column >= 0 && depth_column < stretched_depth_mat_.cols && row >= 0 && row < stretched_depth_mat_.rows)
    //   {
    //     auto depth = (range * cos(measurement_angle));
    //     if (depth > 0.0) // Negative values are possible if the FoV is more than 180 degrees and the laser measures behind it's center plane
    //     {
    //       auto depth_value = depth * 1000;  // +1 value means 1mm further away
    //       // ROS_INFO_STREAM("scan_in.ranges["<<i<<"] ("<< measurement_angle << " rad): range: " << range << ", depth: " << depth << ", (uint16_t)depth_value: " << (uint16_t)depth_value << ", depth_column: " << depth_column);
    //       stretched_depth_mat_.at<uint16_t>(row, depth_column) = (uint16_t)depth_value;
    //     }
    //   }
    //   else
    //   {
    //     ROS_DEBUG("Row and/or column outside of depth image");
    //   }
    // }
  
    // New implementation that also does interpolation
    // ROS_INFO_STREAM("Current height: " << height << ", current index: " << scan_index_);
    height_values_.push_back<double>(height);
    // ROS_INFO_STREAM("height_values_.size: " << height_values_.size);
    // std::cout << "Before push_back: scan_buffer_: " << std::endl << scan_buffer_ << std::endl;

    // std::vector<uint16_t> converted_ranges(scan_in.ranges.begin(), scan_in.ranges.end());

    // ROS_INFO_STREAM("Initializing current_ranges from converted_ranges");
    // cv::Mat current_ranges = cv::Mat(converted_ranges, true);
    // ROS_INFO_STREAM("current_ranges *= 1000");
    // current_ranges *= 1000;  // From meters to millimeters
    // // std::cout << "current_ranges = " << current_ranges << std::endl;

    // auto roi = cv::Rect(scan_index_, 0, scan_in.ranges.size(), 1);
    // ROS_INFO_STREAM("Initializing ranges2 as ROI in scan_buffer_" << roi);
    // cv::Mat ranges2 = cv::Mat(scan_buffer_, roi);
    // ROS_INFO_STREAM("ranges2.shape: " << ranges2.size);
    // ROS_INFO_STREAM("current_ranges.copyTo(ranges2)");
    // current_ranges.copyTo(ranges2);
    // ROS_INFO_STREAM("current_ranges.shape: " << current_ranges.size << ", scan_buffer_.shape: " << scan_buffer_.size);
    
    // TODO: Make this much faster without loop
    for (size_t i = 0; i < scan_in.ranges.size(); i++)
    {
      scan_range_buffer_.at<uint16_t>(scan_index_, i) = (uint16_t)(scan_in.ranges[i] * 1000);

      auto measurement_angle = scan_in.angle_min + i*scan_in.angle_increment;
      auto depth_x_distance = scan_in.ranges[i] * sin(measurement_angle);
      uint depth_column = (depth_x_distance - depth_min_x) / depth_horizontal_step;

      auto depth = scan_in.ranges[i] * cos(measurement_angle);
      if(depth > 0 && depth_column >= 0 && depth_column < scan_depth_buffer_.cols)
      {
        // Can only process positive depth value, cannot look behind us really
        auto depth_value = (uint16_t)(depth * 1000);
        auto curr_value = scan_depth_buffer_.at<uint16_t>(scan_index_, depth_column);
        auto min_value = curr_value > 0 ? std::min(depth_value, curr_value) : depth_value;
        scan_depth_buffer_.at<uint16_t>(scan_index_, depth_column) = min_value;

        ROS_INFO_STREAM_COND(0==0,
          "i: " << i <<
          ", height: " << height <<
          ", range: " << scan_in.ranges[i] <<
          ", measurement_angle: " << measurement_angle <<
          ", depth_x_distance: " << depth_x_distance <<
          ", depth_column: " << depth_column <<
          ", depth_min_x: " << depth_min_x <<
          ", depth_horizontal_step: " << depth_horizontal_step <<
          ", depth: " << depth <<
          ", depth_value: " << depth_value <<
          ", curr_value: " << curr_value <<
          ", min_value: " << min_value <<
          "");
      }
      else
      {
        ROS_INFO_STREAM("Not writing to depth_column " << depth_column << ", range=" << scan_in.ranges[i] << ", measurement_angle=" << measurement_angle);
      }
    }
    // ROS_INFO_STREAM("scan_buffer_.at(" << scan_index_ << ", " << 0 << ") = " << scan_buffer_.at<uint16_t>(scan_index_, 0) << ", scan_in.ranges[0] = " << scan_in.ranges[0]);

    // std::cout << "After  push_back: scan_buffer_: " << std::endl << scan_buffer_ << std::endl;
    // std::cout << "stretched_depth_mat_: " << std::endl << stretched_depth_mat_ << std::endl;
    std::cout << "New row at " << scan_index_ <<" (height=" << height << "): " << std::endl << scan_depth_buffer_.row(scan_index_) << std::endl;

    scan_index_++;
    // ROS_INFO_STREAM("scan_in.ranges.size(): " << scan_in.ranges.size() <<
    //   ", (uint)scan_in.ranges.size()" << (uint)scan_in.ranges.size() <<
    //   ", max_scan_length_" << max_scan_length_ <<
    //   ", std::max((uint)scan_in.ranges.size(), max_scan_length_" << std::max((uint)scan_in.ranges.size(), max_scan_length_));
    max_scan_length_ = std::max((uint)scan_in.ranges.size(), max_scan_length_);
    last_scan_ = scan_in; // TODO: How to better keep track of this metadata? Should be the same for all scans of course, but not guaranteed. This is assumed in the buffer as well of course
  }

  void ConvertToCloud(const string& fixed_frame_id, const sensor_msgs::LaserScan& scan_in, sensor_msgs::PointCloud& cloud_out)
  {
    // apply filters on laser scan
    filter_chain_.update (scan_in, scan_filtered_);

    // convert laser scan to point cloud
    if (ignore_laser_skew_)  // Do it the fast (approximate) way
    {
      projector_.projectLaser(scan_filtered_, cloud_out);
      if (cloud_out.header.frame_id != fixed_frame_id)
        tf_->transformPointCloud(fixed_frame_id, cloud_out, cloud_out);
    }
    else                     // Do it the slower (more accurate) way
    {
      int mask = laser_geometry::channel_option::Intensity +
                 laser_geometry::channel_option::Distance +
                 laser_geometry::channel_option::Index +
                 laser_geometry::channel_option::Timestamp;
      projector_.transformLaserScanToPointCloud (fixed_frame_id, scan_filtered_, cloud_out, *tf_, mask);
    }

    // Only do this when startCollection has set these Mat's to a useable value
    if (!stretched_range_mat_.empty() && !stretched_depth_mat_.empty())
    {
      ScanToImages(fixed_frame_id, scan_in);
    }

    if (!scan_range_buffer_.empty())
    {
    }

    return;
  }

  void scanCallback(const sensor_msgs::LaserScanConstPtr& laser_scan)
  {
    if (!ignore_laser_skew_)
    {
      ros::Duration cur_tolerance = ros::Duration(laser_scan->time_increment * laser_scan->ranges.size());
      if (cur_tolerance > max_tolerance_)
      {
        ROS_DEBUG("Upping tf tolerance from [%.4fs] to [%.4fs]", max_tolerance_.toSec(), cur_tolerance.toSec());
        assert(tf_filter_);
        tf_filter_->setTolerance(cur_tolerance);
        max_tolerance_ = cur_tolerance;
      }
      tf_filter_->add(laser_scan);
    }
  }

  bool startCollection(StartCollection::Request& req, StartCollection::Response& resp)
  {
    scan_hist_mutex_.lock();
    scan_hist_.clear();
    scan_hist_mutex_.unlock();

    current_req_ = req;
    stretched_range_image_ = sensor_msgs::Image();
    // stretched_range_image_.encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    // stretched_range_image_.width = req.horizontal_resolution;
    // stretched_range_image_.height = req.vertical_resolution;
    // stretched_range_image_.step = stretched_range_image_.width;
    // stretched_range_image_.data.resize(stretched_range_image_.width * stretched_range_image_.height);
    stretched_range_image_.header.frame_id = fixed_frame_.c_str();
    ROS_WARN_COND(req.horizontal_resolution == 0, "horizontal_resolution is zero");
    ROS_WARN_COND(req.vertical_resolution == 0, "vertical_resolution is zero");
    // Set uninitialized range to max of the datatype, so that we can take the minimum value of current 
    // and potentially multiple measurements striking the same pixel
    stretched_range_mat_ = cv::Mat::zeros(req.vertical_resolution, req.horizontal_resolution, CV_16UC1);
    ROS_DEBUG_STREAM("Created image to be filled by scans:\n" << stretched_range_image_);

    stretched_depth_mat_ = cv::Mat::zeros(req.vertical_resolution, req.horizontal_resolution, CV_16UC1);
    ROS_DEBUG_STREAM("Created image to be filled by scans:\n" << stretched_depth_image_);


    scan_range_buffer_ = cv::Mat::zeros(1000, 1000, CV_16UC1);
    scan_depth_buffer_ = cv::Mat::zeros(1000, req.horizontal_resolution, CV_16UC1);
    scan_index_ = 0;
    height_values_ = cv::Mat::zeros(1000, 0, CV_64FC1);

    subscribe();
    return true;
  }

  bool stopCollectionAndAssembleScans2(StopCollectionAndAssembleScans2::Request& req, StopCollectionAndAssembleScans2::Response& resp)
  {
    unsubscribe();

    scan_hist_mutex_.lock();

    sensor_msgs::PointCloud cloud;
    bool success = assembleScanIndices(cloud, 0, scan_hist_.size());
    resp.success = success;
    if (success)
    {
      ROS_INFO_STREAM_NAMED("stopCollectionAndAssembleScans2", "Cloud has " << cloud.points.size() << " points from " << scan_hist_.size() << " scans");
      sensor_msgs::PointCloud2 cloud2;
      sensor_msgs::convertPointCloudToPointCloud2(cloud, cloud2);
      pointcloud2_pub_.publish(cloud2);
    }
    else
    {
      ROS_INFO_STREAM_NAMED("stopCollectionAndAssembleScans2", "Could not assemble scans");
    }

    // std::cout << "stretched_range_mat_: " << std::endl << stretched_range_mat_ << std::endl;
    // std::cout << "stretched_depth_mat_: " << std::endl << stretched_depth_mat_ << std::endl;

    std::vector<double> sorted_heights;

    // Process range image

    cv_bridge::CvImage cvi_range_mat;
    cvi_range_mat.encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    auto filled_roi = cv::Rect(0, 0, max_scan_length_, scan_index_);
    ROS_DEBUG_STREAM("Buffer has size " << scan_range_buffer_.size() << "and the filled region of that has size " << filled_roi);
    cv::Mat cropped = cv::Mat(scan_range_buffer_, filled_roi);
    std::multimap<double, std::size_t> reordering;
    cv::Mat sorted = sortMatRowBy(height_values_, cropped, sorted_heights, reordering);

    // Generate y_map for use in cv::remap
    ROS_DEBUG("Create y_map_column");
    cv::Mat y_map_column = cv::Mat::zeros(current_req_.vertical_resolution, 1, CV_32FC1);  // We'll stretch it later, as all the values in a row are the same for Y
    auto vertical_step = fabs(current_req_.max_height - current_req_.min_height) / current_req_.vertical_resolution;
    for (size_t y = 0; y < current_req_.vertical_resolution; y++)
    {
      auto height_at_pixel = current_req_.min_height + (y*vertical_step);
      auto row_in_buffer = findInterpolatedIndex(sorted_heights, height_at_pixel);
      y_map_column.at<float>(y, 0) = (float)row_in_buffer;
      ROS_INFO_STREAM("y: " << y << 
        ", height_at_pixel: " << height_at_pixel << 
        ", row_in_buffer:" << (float)row_in_buffer);
    }
    cv::Mat y_map;
    cv::resize(y_map_column, y_map, cv::Size(current_req_.horizontal_resolution, current_req_.vertical_resolution), cv::INTER_NEAREST);

    // Generate x map for use in cv::remap
    ROS_DEBUG("Create x_map_row");
    cv::Mat x_map_row = cv::Mat::zeros(1, current_req_.horizontal_resolution, CV_32FC1);  // We'll stretch it later, as all the values in a column are the same for X
    auto horizontal_step = fabs(current_req_.max_width - current_req_.min_width) / current_req_.horizontal_resolution;
    for (size_t x = 0; x < current_req_.horizontal_resolution; x++)
    {
      auto angle_at_pixel = current_req_.min_width + (x*horizontal_step);
      auto column_in_buffer = (angle_at_pixel - last_scan_.angle_min) / last_scan_.angle_increment;
      x_map_row.at<float>(0, x) = (float)column_in_buffer;
    }
    cv::Mat x_map;
    cv::resize(x_map_row, x_map, cv::Size(current_req_.horizontal_resolution, current_req_.vertical_resolution), cv::INTER_NEAREST);

    cv::Mat remapped_buffer = cv::Mat::zeros(current_req_.vertical_resolution, current_req_.horizontal_resolution, CV_16UC1);
    ROS_DEBUG("Apply remapping");
    cv::remap(sorted, remapped_buffer, x_map, y_map, cv::INTER_LINEAR, cv::BORDER_CONSTANT, 0);

    // Process depth image
    auto filled_depth_roi = cv::Rect(0, 0, current_req_.horizontal_resolution, scan_index_);
    ROS_INFO_STREAM("Buffer has size " << scan_range_buffer_.size() << "and the filled region of that has size " << filled_roi);
    cv::Mat cropped_depth = cv::Mat(scan_depth_buffer_, filled_depth_roi);
    ROS_INFO_STREAM("Cropped_depth has size " << cropped_depth.size());
    cv::Mat sorted_depth = reorderImageRows(cropped_depth, reordering);
    ROS_INFO_STREAM("sorted_depth has size " << sorted_depth.size());
    std::cout << "sorted_depth" << std::endl << sorted_depth << std::endl;

    cvi_range_mat.image = remapped_buffer;
    cvi_range_mat.toImageMsg(stretched_range_image_);
    stretched_range_image_.header.stamp = ros::Time::now();
    stretched_range_image_.header.frame_id = fixed_frame_.c_str();
    stretched_range_image_pub_.publish(stretched_range_image_);

    cv::Mat depth_x_map_row = cv::Mat::zeros(1, current_req_.horizontal_resolution, CV_32FC1);  // We'll stretch it later, as all the values in a column are the same for X
    for (size_t x = 0; x < current_req_.horizontal_resolution; x++)
    {
      // Depths are already in the right column, that is done in ScanToImages where the appropriate trigonometry is perfomed
      depth_x_map_row.at<float>(0, x) = (float)x;
    }
    // std::cout << "depth_x_map_row" << std::endl << depth_x_map_row << std::endl;

    cv::Mat depth_x_map;
    cv::resize(depth_x_map_row, depth_x_map, cv::Size(current_req_.horizontal_resolution, current_req_.vertical_resolution), cv::INTER_NEAREST);
    ROS_INFO_STREAM("depth_x_map has size " << depth_x_map.size());
    // std::cout << "depth_x_map" << std::endl << depth_x_map << std::endl;

    // Depth can use the same y map, since the ordering of the rows is the same and the output resolution is the same
    std::cout << "y_map" << std::endl << y_map << std::endl;

    cv::Mat remapped_depth_buffer = cv::Mat::zeros(current_req_.vertical_resolution, current_req_.horizontal_resolution, CV_16UC1);
    ROS_DEBUG("Apply remapping");
    cv::remap(sorted_depth, remapped_depth_buffer, depth_x_map, y_map, cv::INTER_LINEAR, cv::BORDER_CONSTANT, 0);
    ROS_INFO_STREAM("remapped_depth_buffer has size " << remapped_depth_buffer.size());
    std::cout << "remapped_depth_buffer" << std::endl << remapped_depth_buffer << std::endl;

    cv_bridge::CvImage cvi_depth_mat;
    cvi_depth_mat.encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    cvi_depth_mat.image = remapped_depth_buffer;
    cvi_depth_mat.toImageMsg(stretched_depth_image_);
    stretched_depth_image_.header.stamp = ros::Time::now();
    stretched_depth_image_.header.frame_id = fixed_frame_.c_str();
    stretched_depth_image_pub_.publish(stretched_depth_image_);

    // No need for theis data anymore, release the memory and make the Mats empty
    stretched_range_mat_.release();
    stretched_depth_mat_.release();

    scan_hist_mutex_.unlock();

    return true;
  }

private:
  bool ignore_laser_skew_;
  bool subscribe_directly_;
  laser_geometry::LaserProjection projector_;

  ros::Subscriber skew_scan_sub_;
  ros::Duration max_tolerance_;   // The longest tolerance we've needed on a scan so far
  ros::ServiceServer start_collection_server_;
  ros::ServiceServer stop_collection_server_;
  StartCollection::Request current_req_;
  ros::Publisher pointcloud2_pub_;
  ros::Publisher stretched_range_image_pub_;
  ros::Publisher stretched_depth_image_pub_;

  cv::Mat scan_range_buffer_;
  cv::Mat scan_depth_buffer_;
  uint scan_index_;
  cv::Mat height_values_;
  uint max_scan_length_ = 0;
  sensor_msgs::LaserScan last_scan_;

  cv::Mat stretched_range_mat_;
  sensor_msgs::Image stretched_range_image_;

  cv::Mat stretched_depth_mat_;
  sensor_msgs::Image stretched_depth_image_;

  filters::FilterChain<sensor_msgs::LaserScan> filter_chain_;
  mutable sensor_msgs::LaserScan scan_filtered_;
};

}

using namespace laser_assembler ;

int main(int argc, char **argv)
{
  ros::init(argc, argv, "laser_scan_assembler");
  LaserScanAssembler pc_assembler;
  ros::spin();

  return 0;
}
