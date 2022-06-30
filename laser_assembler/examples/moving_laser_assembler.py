#! /usr/bin/env python

import argparse
from actionlib.simple_action_client import SimpleActionClient
from control_msgs.msg import (
    FollowJointTrajectoryAction,
    FollowJointTrajectoryGoal,
    FollowJointTrajectoryResult,
)
from laser_assembler.srv import (
    StartCollection,
    StartCollectionRequest,
    StartCollectionResponse,
    StopCollectionAndAssembleScans2,
    StopCollectionAndAssembleScans2Request,
    StopCollectionAndAssembleScans2Response,
)
import rospy
from sensor_msgs.msg import PointCloud2, JointState
from std_msgs.msg import Header
from trajectory_msgs.msg import JointTrajectory, JointTrajectoryPoint
from math import pi

if __name__ == "__main__":
    rospy.init_node("assemble_moving_laser")

    parser = argparse.ArgumentParser(
        description="Control the fork and laser assembler in unison"
    )
    parser.add_argument(
        "--scan_start",
        "--scan-start",
        type=float,
        help="Height to start scan at",
        default=0.1,
    )
    parser.add_argument(
        "--scan_end",
        "--scan-end",
        type=float,
        help="Height to end scan at",
        default=2.0,
    )
    parser.add_argument(
        "--velocity",
        "--velocity",
        type=float,
        help="Velocity to move the joint with",
        default=0.5,
    )
    parser.add_argument(
        "--min_width",
        "--min-width",
        type=float,
        help="Left end of the FoV",
        default=-pi,
    )
    parser.add_argument(
        "--max_width",
        "--max-width",
        type=float,
        help="Right end of the FoV",
        default=pi,
    )
    parser.add_argument(
        "--vertical_resolution",
        "--vertical-resolution",
        type=int,
        help="How many pixels in horizontal axis of image",
        default=200,
    )
    parser.add_argument(
        "--horizontal_resolution",
        "--horizontal-resolution",
        type=int,
        help="How many pixels in vertical axis of image",
        default=200,
    )
    args = parser.parse_args(rospy.myargv()[1:])
    print(args)

    component = "mast_lift"
    joint = "mast_lift_joint"

    scan_start = args.scan_start
    scan_end = args.scan_end
    joint_velocity = args.velocity

    scan_duration = rospy.Duration.from_sec(abs(scan_end - scan_start) / joint_velocity)

    assembled_cloud_pub = rospy.Publisher("assembled_cloud", PointCloud2, queue_size=1)

    fjta = SimpleActionClient(
        f"/{component}/joint_trajectory_controller/follow_joint_trajectory",
        FollowJointTrajectoryAction,
    )

    start_srv = rospy.ServiceProxy("/start_collection", StartCollection)
    stop_srv = rospy.ServiceProxy(
        "/stop_collection_and_assemble_scans2", StopCollectionAndAssembleScans2
    )

    rospy.loginfo(f"Waiting for FollowJointTrajectory of {component}")
    fjta.wait_for_server(rospy.Duration(1))
    rospy.loginfo(f"Waiting for scan assembler")
    start_srv.wait_for_service(rospy.Duration(1))
    stop_srv.wait_for_service(rospy.Duration(1))

    rospy.loginfo(f"Waiting for current state of {component}")
    current_state = rospy.wait_for_message(
        "/joint_states", JointState
    )  # type: JointState
    current_state_dict = dict(zip(current_state.name, current_state.position))
    rospy.loginfo(f"Current state of {component} is {current_state_dict}")

    joint_initial = current_state_dict[joint]
    current_to_start_scan_dur = rospy.Duration.from_sec(
        abs(scan_start - joint_initial) / joint_velocity
    )
    end_scan_to_initial_dur = rospy.Duration.from_sec(
        abs(joint_initial - scan_end) / joint_velocity
    )

    to_start_traj = JointTrajectory(
        header=Header(
            stamp=rospy.Time.now() + rospy.Duration.from_sec(0.1),
            frame_id="base_footprint",
        ),
        joint_names=["mast_lift_joint"],
        points=[
            JointTrajectoryPoint(
                time_from_start=rospy.Duration(0), positions=[joint_initial]
            ),
            JointTrajectoryPoint(
                time_from_start=current_to_start_scan_dur, positions=[scan_start]
            ),
        ],
    )
    rospy.loginfo("Move to start of scan")
    fjta.send_goal_and_wait(FollowJointTrajectoryGoal(trajectory=to_start_traj))

    rospy.loginfo("Starting scan movement")

    scan_traj = JointTrajectory(
        header=Header(
            stamp=rospy.Time.now() + rospy.Duration.from_sec(0.1),
            frame_id="base_footprint",
        ),
        joint_names=["mast_lift_joint"],
        points=[
            JointTrajectoryPoint(
                time_from_start=rospy.Duration(0), positions=[scan_start]
            ),
            JointTrajectoryPoint(time_from_start=scan_duration, positions=[scan_end]),
        ],
    )
    scan_movement_start = rospy.Time.now()
    start_srv(
        min_height=scan_start,
        max_height=scan_end,
        min_width=args.min_width,
        max_width=args.max_width,
        vertical_resolution=args.vertical_resolution,
        horizontal_resolution=args.horizontal_resolution,
    )
    # Wait for a result for the total movement time + some margin
    fjta.send_goal_and_wait(
        FollowJointTrajectoryGoal(trajectory=scan_traj),
        execute_timeout=scan_traj.points[-1].time_from_start + rospy.Duration(5),
    )
    rospy.loginfo("Scan movement finished, stopping collection of scans")
    stop_srv()

    move_result = fjta.get_result()
    scan_movement_end = rospy.Time.now()  # Only really the end if move_result == True!

    rospy.loginfo("Moving back to initial")
    back_to_initial_traj = JointTrajectory(
        header=Header(
            stamp=rospy.Time.now() + rospy.Duration.from_sec(0.1),
            frame_id="base_footprint",
        ),
        joint_names=["mast_lift_joint"],
        points=[
            JointTrajectoryPoint(
                time_from_start=rospy.Duration(0), positions=[scan_end]
            ),
            JointTrajectoryPoint(
                time_from_start=end_scan_to_initial_dur, positions=[joint_initial]
            ),
        ],
    )

    fjta.send_goal(FollowJointTrajectoryGoal(trajectory=back_to_initial_traj))
    # if move_result:
    #     if move_result and move_result.error_code == FollowJointTrajectoryResult.SUCCESSFUL:
    #         rospy.loginfo("Starting scan assembler")
    #         assemble_response = assemble_srv(
    #             begin=scan_movement_start, end=scan_movement_end
    #         )  # type: AssembleScans2Response
    #         rospy.loginfo("Got result, will publish")
    #         rospy.loginfo("Width: {}".format(assemble_response.cloud.width))
    #         rospy.loginfo("Height: {}".format(assemble_response.cloud.height))
    #
    #         assembled_cloud_pub.publish(assemble_response.cloud)
    #     else:
    #         rospy.logerr(f"Movement not succeeded: {move_result.error_code}")
    # else:
    #     rospy.logerr(f"Movement not yet done: {move_result.error_code}")
    fjta.wait_for_result()
    rospy.loginfo("Arrived back at initial state, all movement done")
