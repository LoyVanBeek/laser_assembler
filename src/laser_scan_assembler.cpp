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

  unsigned int GetPointsInScan(const sensor_msgs::LaserScan& scan)
  {
    return (scan.ranges.size ());
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

    if (!stretched_range_mat_.empty() && !stretched_depth_mat_.empty())
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
      ROS_INFO_STREAM("current_req_.max_range: " << current_req_.max_range << ", current_req_.min_width: " << current_req_.min_width << ", sin(current_req_.min_width): " << sin(current_req_.min_width));
      auto depth_width_x = abs(depth_max_x - depth_min_x);  // [m] Width of the image in meters
      auto depth_max_val = current_req_.max_range;  // [m]Maximum depth represented in the depth image

      auto vertical_step = abs(current_req_.max_height - current_req_.min_height) / current_req_.vertical_resolution;  // [m] Height of a pixel in both depth and range image
      auto range_horizontal_step = abs(current_req_.max_width - current_req_.min_width) / current_req_.horizontal_resolution;  // [rad] angle covered by a pixel in the range image
      auto depth_horizontal_step = depth_width_x / current_req_.horizontal_resolution;  // [m] Width of a pixel in the depth image
      auto range_step = abs(current_req_.max_range - current_req_.min_range) / std::numeric_limits<ushort>::max();  // [m] how much further away is a pixel of +1 value in the range image
      auto depth_step = depth_max_val / std::numeric_limits<ushort>::max();  // [m] how much further away is a pixel of +1 value in the depth image?

      tf::StampedTransform transform;
      tf_->lookupTransform(fixed_frame_id, scan_in.header.frame_id, ros::Time(0), transform);
      auto height = transform.getOrigin().z(); // TODO: Make this bit configurable to be generic
      uint row = stretched_range_mat_.rows - ((height - current_req_.min_height) / vertical_step);  // TODO: check proper types, absolutes and rounding
      // ROS_INFO_STREAM("height: " << height << ", max_height: " << current_req_.max_height << ", min_height: " << current_req_.min_height << ", vertical_step: " << vertical_step << ", row: " << row);
      // ROS_INFO_STREAM("max_width: " << current_req_.max_width << ", min_width: " << current_req_.min_width << ", horizontal_step: " << horizontal_step);
      // ROS_INFO_STREAM("scan_in.range_max: " << scan_in.range_max << ", scan_in.range_min: " << scan_in.range_min << ", std::numeric_limits<short>::max(): " << std::numeric_limits<ushort>::max() << ", range_step: " << range_step);
      ROS_INFO_STREAM("depth_min_x: " << depth_min_x << ", depth_max_x: " << depth_max_x << ", depth_width_x: " << depth_width_x << ", depth_max_val: " << depth_max_val << ", depth_horizontal_step: " << depth_horizontal_step << ", depth_step: " << depth_step);

      for (size_t i = 0; i < scan_in.ranges.size(); i++)
      {
        auto range = scan_in.ranges[i];
        auto measurement_angle = scan_in.angle_min + i*scan_in.angle_increment;

        auto depth_x_distance = range * sin(measurement_angle);

        uint range_column = (measurement_angle - current_req_.min_width) / range_horizontal_step;
        uint depth_column = (depth_x_distance - depth_min_x) / depth_horizontal_step;
        // ROS_DEBUG_STREAM("column: " << column << ", index: " << i << ", measurement_angle: " << measurement_angle << ", max_width: " << current_req_.max_width << ", min_width: " << current_req_.min_width << ", horizontal_step: " << horizontal_step);
        ROS_INFO_STREAM_COND(i==i, "i: " << i << ", row: " << row << ", range_column: " << range_column << ", range: " << range << ", depth_column: " << depth_column);
        if (range_column >= 0 && range_column < stretched_range_mat_.cols && row >= 0 && row < stretched_range_mat_.rows)
        {
          if (range < current_req_.min_range)
          {
            stretched_range_mat_.at<ushort>(row, range_column) = 0;
          }
          else
          {
            auto range_value = (range - scan_in.range_min) / range_step;
            // ROS_DEBUG_STREAM("scan_in.ranges["<<i<<"]: " << range << ", value: " << value << ", (ushort)value: " << (ushort)value);

            // If a pixel already has a value (which is always the case, since pixels are initialized to max range),
            // take the minimum value of the new and current
            stretched_range_mat_.at<ushort>(row, range_column) = std::min((ushort)range_value, stretched_range_mat_.at<ushort>(row, range_column));
          }
        }
        else
        {
          ROS_DEBUG("Row and/or column outside of range image");
        }

        if (depth_column >= 0 && depth_column < stretched_depth_mat_.cols && row >= 0 && row < stretched_depth_mat_.rows)
        {
          auto depth = (range * cos(measurement_angle));
          if (depth > 0.0) // Negative values are possible if the FoV is more than 180 degrees and the laser measures behind it's center plane
          {
            auto depth_value = depth / depth_step;
            ROS_INFO_STREAM_COND(i==i, "scan_in.ranges["<<i<<"] ("<< measurement_angle << " rad): range: " << range << ", depth: " << depth << ", (ushort)depth_value: " << (ushort)depth_value << ", depth_column: " << depth_column);
            stretched_depth_mat_.at<ushort>(row, depth_column) = std::min((ushort)depth, stretched_depth_mat_.at<ushort>(row, depth_column));
          }
        }
        else
        {
          ROS_DEBUG("Row and/or column outside of depth image");
        }
      }
    }
    else
    {
      ROS_INFO("No stretched_range_mat_ or stretched_depth_mat_to fill");
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
    stretched_range_mat_ = cv::Mat::ones(req.vertical_resolution, req.horizontal_resolution, CV_16UC1) * std::numeric_limits<ushort>::max(); 
    ROS_DEBUG_STREAM("Created image to be filled by scans:\n" << stretched_range_image_);

    stretched_depth_mat_ = cv::Mat::ones(req.vertical_resolution, req.horizontal_resolution, CV_16UC1) * std::numeric_limits<ushort>::max();
    ROS_DEBUG_STREAM("Created image to be filled by scans:\n" << stretched_depth_image_);

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

    scan_hist_mutex_.unlock();

    cv_bridge::CvImage cvi_mat;
    cvi_mat.encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    cvi_mat.image = stretched_range_mat_;
    cvi_mat.toImageMsg(stretched_range_image_);
    stretched_range_image_.header.stamp = ros::Time::now();
    stretched_range_image_.header.frame_id = fixed_frame_.c_str();
    stretched_range_image_pub_.publish(stretched_range_image_);

    cvi_mat.image = stretched_depth_mat_;
    cvi_mat.toImageMsg(stretched_depth_image_);
    stretched_depth_image_.header.stamp = ros::Time::now();
    stretched_depth_image_.header.frame_id = fixed_frame_.c_str();
    stretched_depth_image_pub_.publish(stretched_depth_image_);

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
