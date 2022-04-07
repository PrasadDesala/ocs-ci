import logging
from ocs_ci.ocs.longevity import start_ocp_workload
from ocs_ci.framework.testlib import E2ETest


log = logging.getLogger(__name__)


class TestLongevity(E2ETest):
    """
    Test class for Longevity: Stage-1
    """

    def test_stage1(self, start_apps_workload):
        """
        This test starts Longevity Stage1
        In Stage 1, we configure and run both OCP and APP workloads
        Detailed steps:
        OCP workloads
        1) Configure openshift-monitoring backed by OCS RBD PVCs
        2) Configure openshift-logging backed by OCS RBD PVCs
        3) Configure openshift-registry backed by OCS CephFs PVC
        APP workloads
        1) Configure and run APP workloads (Pgsql, Couchbase, Cosbench, Jenkins, etc)
        2) Repeat Step-1 and run the workloads continuously for a specified period

        """
        # Start stage-1
        log.info("Starting Longevity Stage-1")
        # Configure OCP workloads
        log.info("Configuring OCP workloads")
        start_ocp_workload(
            workloads_list=["monitoring", "registry", "logging"], run_in_bg=True
        )
        # Start application workloads and continuously for the specified period
        log.info("Start running application workloads")
        start_apps_workload(
            workloads_list=["couchbase", "cosbench"], run_time=180, run_in_bg=True
        )
