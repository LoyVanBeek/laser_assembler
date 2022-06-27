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

    // Automatically subscribe to scans or wait for start-service?
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
      //   ROS_INFO_STREAM(
      // "query: " << query <<
      // ", index_under: " << index_under <<
      // ", value_under: " << value_under <<
      // ", index_above: " << index_above <<
      // ", value_above: " << value_above <<
      // ", interpolated_index: " << interpolated_index);
    return interpolated_index;
  }

  unsigned int GetPointsInScan(const sensor_msgs::LaserScan& scan)
  {
    return (scan.ranges.size ());
  }

  void ScanToImages(const string& fixed_frame_id, const sensor_msgs::LaserScan& scan_in)
  {
    tf::StampedTransform transform;
    tf_->lookupTransform(fixed_frame_id, scan_in.header.frame_id, ros::Time(0), transform);
    auto height = (double)transform.getOrigin().z(); // TODO: Make this bit configurable to be generic
    ROS_DEBUG_STREAM("height: " << height << ", max_height: " << current_req_.max_height << ", min_height: " << current_req_.min_height);
    ROS_DEBUG_STREAM("max_width: " << current_req_.max_width << ", min_width: " << current_req_.min_width);
    ROS_DEBUG_STREAM("scan_in.range_max: " << scan_in.range_max << ", scan_in.range_min: " << scan_in.range_min << ", std::numeric_limits<short>::max(): " << std::numeric_limits<uint16_t>::max());
  
    // New implementation that also does interpolation
    // ROS_INFO_STREAM("Current height: " << height << ", current index: " << scan_index_);
    height_values_.push_back<double>(height);
    // ROS_INFO_STREAM("height_values_.size: " << height_values_.size);
    // std::cout << "Before push_back: scan_buffer_: " << std::endl << scan_buffer_ << std::endl;

    // std::vector<float> ranges(scan_in.ranges.begin(), scan_in.ranges.end());

    // ROS_INFO_STREAM("Initializing current_ranges from converted_ranges");
    // cv::Mat ranges_mat = cv::Mat(ranges, true);
    // ranges_mat *= 1000;  // From meters to millimeters
    // cv::Mat ranges_as_uint16;
    // ranges_mat.convertTo(ranges_as_uint16, CV_16UC1);
    // // ROS_INFO_STREAM("ranges_mat *= 1000");
    // std::cout << "ranges_as_uint16 = " << ranges_as_uint16.t() << std::endl;

    // auto roi = cv::Rect(0, scan_index_, scan_in.ranges.size(), 1);
    // ROS_INFO_STREAM("Initializing ranges2 as ROI in scan_range_buffer_" << roi);
    // cv::Mat ranges2 = cv::Mat(scan_range_buffer_, roi);
    // ROS_INFO_STREAM("ranges2.shape: " << ranges2.size);
    // ROS_INFO_STREAM("ranges_as_uint16.copyTo(ranges2)");
    // ranges_as_uint16.copyTo(ranges2);
    // ROS_INFO_STREAM("ranges_as_uint16.shape: " << ranges_as_uint16.size << ", scan_buffer_.shape: " << scan_range_buffer_.size);
    // std::cout << "ranges2 = " << ranges2.t() << std::endl;
    
    // TODO: Make this much faster without loop
    for (size_t i = 0; i < scan_in.ranges.size(); i++)
    {
      scan_range_buffer_.at<uint16_t>(scan_index_, i) = (uint16_t)(scan_in.ranges[i] * 1000);
    }
    // ROS_INFO_STREAM("scan_buffer_.at(" << scan_index_ << ", " << 0 << ") = " << scan_buffer_.at<uint16_t>(scan_index_, 0) << ", scan_in.ranges[0] = " << scan_in.ranges[0]);

    // std::cout << "After  push_back: scan_buffer_: " << std::endl << scan_buffer_ << std::endl;
    // std::cout << "stretched_depth_mat_: " << std::endl << stretched_depth_mat_ << std::endl;
    // std::cout << "New row at " << scan_index_ <<" (height=" << height << "): " << std::endl << scan_depth_buffer_.row(scan_index_) << std::endl;

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
    if (!scan_range_buffer_.empty())
    {
      ScanToImages(fixed_frame_id, scan_in);
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
    ROS_WARN_COND(req.horizontal_resolution == 0, "horizontal_resolution is zero");
    ROS_WARN_COND(req.vertical_resolution == 0, "vertical_resolution is zero");

    scan_range_buffer_ = cv::Mat::zeros(1000, 1000, CV_16UC1);
    scan_index_ = 0;
    height_values_ = cv::Mat::zeros(1000, 0, CV_64FC1);

    subscribe();

    resp.success = true;
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
      resp.message = "Could not assemble scans";
    }

    std::vector<double> sorted_heights;

    // Process range image
    auto filled_roi = cv::Rect(0, 0, max_scan_length_, scan_index_);
    ROS_DEBUG_STREAM("Buffer has size " << scan_range_buffer_.size() << "and the filled region of that has size " << filled_roi);
    cv::Mat cropped_buffer = cv::Mat(scan_range_buffer_, filled_roi);
    std::multimap<double, std::size_t> reordering;
    //The cropped buffer needs to be sorted before we can use it: 
    // the height of the scans might not be monotonically increaseing/decreasing when moving up AND down betwen start and stop
    cv::Mat sorted_ranges = sortMatRowBy(height_values_, cropped_buffer, sorted_heights, reordering);

    // Generate y_map for use in cv::remap
    ROS_DEBUG("Create y_map_column");
    cv::Mat y_map_column = cv::Mat::zeros(current_req_.vertical_resolution, 1, CV_32FC1);  // We'll stretch it later, as all the values in a row are the same for Y
    auto vertical_step = fabs(current_req_.max_height - current_req_.min_height) / current_req_.vertical_resolution;
    for (size_t y = 0; y < current_req_.vertical_resolution; y++)
    {
      auto height_at_pixel = current_req_.min_height + (y*vertical_step);
      auto row_in_buffer = findInterpolatedIndex(sorted_heights, height_at_pixel);
      // current_req_.vertical_resolution - y because we want the pixels with lower height at the bottom of the image of course
      y_map_column.at<float>(current_req_.vertical_resolution - y, 0) = (float)row_in_buffer;
      // ROS_INFO_STREAM("y: " << y <<
      //   ", height_at_pixel: " << height_at_pixel <<
      //   ", row_in_buffer:" << (float)row_in_buffer);
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

    cv::Mat range_image = cv::Mat::zeros(current_req_.vertical_resolution, current_req_.horizontal_resolution, CV_16UC1);
    ROS_DEBUG("Apply remapping");
    cv::remap(sorted_ranges, range_image, x_map, y_map, cv::INTER_LINEAR, cv::BORDER_CONSTANT, 0);

    // Convert the range image to depth image by multiplying it by cos(angle)
    // First we create an image containing the angle at each image column

    // Angles, step 1: for each pixel, calculate the angle and take the cos(angle)
    cv::Mat cos_angle_row = cv::Mat::zeros(1, current_req_.horizontal_resolution, CV_32FC1);
    // Calculate cosine of all angles, in place
    cos_angle_row.forEach<float>([this, horizontal_step](float &value, const int* position) -> void {
        auto angle_at_pixel = current_req_.min_width + (position[1]*horizontal_step);
        // std::cout << "position[" << position[0] << ", " << position[1] << "]: " << value << ", angle_at_pixel: " << angle_at_pixel << std::endl;
        value = cos(angle_at_pixel);
    });
    // std::cout << "cos(angle_row): " << cos_angle_row << std::endl;
        
    // Angles, step 2: stretch the row vertically so all output rows are the same
    cv::Mat cos_angles = cv::Mat::zeros(current_req_.vertical_resolution, current_req_.horizontal_resolution, CV_32FC1);
    cv::resize(cos_angle_row, cos_angles, cv::Size(current_req_.horizontal_resolution, current_req_.vertical_resolution), cv::INTER_NEAREST);

    // std::cout << "cos_angles: " << cos_angles << std::endl;
    
    ROS_DEBUG("remapped_buffer.convertTo(range_as_float, CV_32FC1)");
    cv::Mat range_as_float;
    range_image.convertTo(range_as_float, CV_32FC1);
    // std::cout << "multiplying depth = range_as_float (size: " << range_as_float.size() << ") * cos_angles (size: " << cos_angles.size() << ")" << std::endl;
    cv::Mat depth_float = range_as_float.mul(cos_angles);
    // std::cout << "depth_float: " << depth_float << std::endl;

    cv::Mat depth_as_uint16;
    depth_float.convertTo(depth_as_uint16, CV_16UC1);
    // std::cout << "depth_as_uint16: " << depth_as_uint16 << std::endl;

    sensor_msgs::Image stretched_range_image;
    cv_bridge::CvImage cvi_range_mat;
    cvi_range_mat.encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    cvi_range_mat.image = range_image;
    cvi_range_mat.toImageMsg(stretched_range_image);
    stretched_range_image.header.stamp = ros::Time::now();
    stretched_range_image.header.frame_id = fixed_frame_.c_str();
    stretched_range_image_pub_.publish(stretched_range_image);

    sensor_msgs::Image stretched_depth_image;
    cv_bridge::CvImage cvi_depth_mat;
    cvi_depth_mat.encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    cvi_depth_mat.image = depth_as_uint16;
    cvi_depth_mat.toImageMsg(stretched_depth_image);
    stretched_depth_image.header.stamp = ros::Time::now();
    stretched_depth_image.header.frame_id = fixed_frame_.c_str();
    stretched_depth_image_pub_.publish(stretched_depth_image);

    // No need for this data anymore, release the memory and make the Mats empty
    scan_range_buffer_.release();

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
  uint scan_index_;
  cv::Mat height_values_;
  uint max_scan_length_ = 0;
  sensor_msgs::LaserScan last_scan_;

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
