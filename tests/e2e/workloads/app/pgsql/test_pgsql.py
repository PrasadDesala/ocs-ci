import logging
import pytest
import random
import string
import re

from datetime import datetime, timedelta
from ocs_ci.ocs import constants
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.framework.testlib import E2ETest, workloads, google_api_required
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@workloads
@pytest.mark.polarion_id("OCS-807")
class TestPgSQLWorkload(E2ETest):
    """
    Deploy an PGSQL workload using operator
    """
    run_time = 14400
    total_rows = 1000

    def test_pgsql_without_pgbench(self, pgsql):

        def run_pgsql_command(postgres_pod, command, select=False):
            res = postgres_pod.exec_cmd_on_pod(f'psql -U postgres -c "{command}" ')
            if select:
                ind = res.index("-")
                result = list()
                result.append("\n" + res[:ind])
                result.append("-" * len(result[0]))
                result.extend(re.findall(r"\d+\s\|\s[A-Z0-9]+\s\|\s\d{4}-\d{2}-\d{2}\s", res))
                result.append("-" * len(result[0]))
                log.info("\n".join(result))
                return
            log.info(res)

        def generate_random_date(min_year=1900, max_year=datetime.now().year):
            # generate a datetime in format yyyy-mm-dd
            start = datetime(min_year, 1, 1, 00, 00, 00)
            years = max_year - min_year + 1
            end = start + timedelta(days=365 * years)
            return (start + (end - start) * random.random()).strftime("%Y-%m-%d")

        def generate_random_string(length):
            return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

        # Deployment postgres
        pgsql.setup_postgresql(replicas=1)
        pgsql.wait_for_postgres_status()
        postgres_pod = pgsql.get_postgres_pods()[0]
        run_pgsql_command(postgres_pod, "\\c testdb")
        run_pgsql_command(
            postgres_pod,
            "CREATE TABLE testing1 ( row_id INT PRIMARY KEY, username VARCHAR ( 50 ) NOT NULL, date DATE NOT NULL);",
        )
        for row_num in range(self.total_rows):
            random_username = generate_random_string(50)
            run_pgsql_command(
                postgres_pod,
                f"INSERT INTO testing1 VALUES ({row_num + 1}, '{random_username}', '{generate_random_date()}');",
            )
        run_pgsql_command(postgres_pod, "SELECT * FROM testing1;", True)

        end_time = datetime.now() + timedelta(minutes=self.run_time)
        while datetime.now() < end_time:
            sql_operation = random.randint(0, 3)
            if sql_operation == 0:
                run_pgsql_command(
                    postgres_pod,
                    (
                        f"INSERT INTO testing1 VALUES ({self.total_rows + 1}, "
                        f"'{generate_random_string(50)}', '{generate_random_date()}');"
                    ),
                )
                self.total_rows += 1
            elif sql_operation == 1:
                run_pgsql_command(
                    postgres_pod,
                    (
                        f"UPDATE testing1 SET username='{generate_random_string(50)}' "
                        f"WHERE row_id={random.randint(1, self.total_rows)};"
                    ),
                )
            elif sql_operation == 2:
                run_pgsql_command(
                    postgres_pod,
                    (
                        f"UPDATE testing1 SET date='{generate_random_date()}' "
                        f"WHERE row_id={random.randint(1, self.total_rows)};"
                    ),
                )
            else:
                run_pgsql_command(
                    postgres_pod,
                    f"DELETE FROM testing1 WHERE row_id={random.randint(1, self.total_rows)};",
                )
            run_pgsql_command(postgres_pod, "SELECT * FROM testing1;", True)
        run_pgsql_command(postgres_pod, "DROP TABLE testing1;")

        # log.info(postgres_pod.exec_cmd_on_pod("psql -U postgres testdb << EOF \\l; select current_user; EOF"))
        # log.info(postgres_pod.exec_cmd_on_pod("echo \'SELECT current_user; CREATE TABLE accounts (
        # user_id serial PRIMARY KEY, username VARCHAR ( 50 ) UNIQUE NOT NULL);\' | psql -U postgres testdb"))

    # def test_sql_workload_simple(self, pgsql):
    #     """
    #     This is a basic pgsql workload
    #     """
    #     # Deployment postgres
    #     pgsql.setup_postgresql(replicas=1)
    #
    #     # Create pgbench benchmark
    #     pgsql.create_pgbench_benchmark(replicas=1, transactions=600)
    #
    #     # Start measuring time
    #     start_time = datetime.now()
    #
    #     # Check worker node utilization (adm_top)
    #     get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)
    #
    #     # Wait for pg_bench pod to initialized and complete
    #     pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)
    #
    #     # Calculate the time from running state to completed state
    #     end_time = datetime.now()
    #     diff_time = end_time - start_time
    #     log.info(
    #         f"\npgbench pod reached to completed state after "
    #         f"{diff_time.seconds} seconds\n"
    #     )
    #
    #     # Get pgbench pods
    #     pgbench_pods = pgsql.get_pgbench_pods()
    #
    #     # Validate pgbench run and parse logs
    #     pg_out = pgsql.validate_pgbench_run(pgbench_pods)
    #
    #     # Export pgdata to google  google spreadsheet
    #     pgsql.export_pgoutput_to_googlesheet(
    #         pg_output=pg_out, sheet_name="E2E Workloads", sheet_index=0
    #     )
