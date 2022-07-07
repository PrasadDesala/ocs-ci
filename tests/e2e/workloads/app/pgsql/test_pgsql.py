import logging
import pytest
import random
import string
import re

from datetime import datetime, timedelta
from ocs_ci.ocs import constants
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.framework.testlib import E2ETest, workloads, google_api_required, ignore_leftovers
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top

log = logging.getLogger(__name__)



@ignore_leftovers
@workloads
@pytest.mark.polarion_id("OCS-807")
class TestPgSQLWorkload(E2ETest):
    """
    Deploy an PGSQL workload using operator
    """
    run_time = 14400
    total_rows = 1000

    def test_pgsql_without_pgbench(self):

        pgsql = Postgresql()
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
                return res
            log.info(res)

        def run_insert_operation(n, postgres_pod, row_index):
            log.info(f"Running {n} insert operations.")
            res = f""
            for row_num in range(row_index + 1, row_index + n + 1):
                random_username = generate_random_string(50)
                res += f"({row_num}, '{random_username}', '{generate_random_date()}'),"
            run_pgsql_command(
                postgres_pod,
                f"INSERT INTO testing3 VALUES {res[:-1]};",
            )

        def run_update_operation(n, postgres_pod, total_rows):
            log.info(f"Running {n} update operations.")
            run_pgsql_command(
                postgres_pod,
                (
                    f"UPDATE testing3 SET username='{generate_random_string(50)}' "
                    f"WHERE row_id in {tuple(random.sample(range(1, total_rows), n))};"
                ),
            )

        def run_delete_operation(n, postgres_pod, total_rows):
            log.info(f"Running {n} delete operations.")
            run_pgsql_command(
                postgres_pod,
                f"DELETE FROM testing3 WHERE row_id in {tuple(random.sample(range(1, total_rows), n))};"
            )

        def generate_random_date(min_year=1900, max_year=datetime.now().year):
            # generate a datetime in format yyyy-mm-dd
            start = datetime(min_year, 1, 1, 00, 00, 00)
            years = max_year - min_year + 1
            end = start + timedelta(days=365 * years)
            return (start + (end - start) * random.random()).strftime("%Y-%m-%d")

        def generate_random_string(length):
            return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

        # Deployment postgres
        try:
            pgsql.setup_postgresql(replicas=1)
        except:
            log.info("PGSQL ALREADY CONFIGURED")
        pgsql.wait_for_postgres_status()
        postgres_pod = pgsql.get_postgres_pods()[0]
        run_pgsql_command(postgres_pod, "\\c testdb")
        run_pgsql_command(
            postgres_pod,
            "CREATE TABLE testing3 ( row_id INT PRIMARY KEY, username VARCHAR ( 50 ) NOT NULL, date DATE NOT NULL);",
        )
        run_insert_operation(self.total_rows, postgres_pod, 0)
        run_pgsql_command(postgres_pod, "SELECT * FROM testing3;", True)

        end_time = datetime.now() + timedelta(minutes=self.run_time)
        while datetime.now() < end_time:
            sql_operation = random.randint(0, 3)
            if sql_operation == 0:
                run_insert_operation(100, postgres_pod, self.total_rows)
                self.total_rows += 100
            elif sql_operation == 1:
                run_update_operation(100, postgres_pod, self.total_rows)
            elif sql_operation == 2:
                run_pgsql_command(
                    postgres_pod,
                    (
                        f"UPDATE testing3 SET date='{generate_random_date()}' "
                        f"WHERE EXTRACT(YEAR FROM date) = {str(random.randint(1900, datetime.now().year))};"
                    ),
                )
            else:
                run_delete_operation(100, postgres_pod, self.total_rows)

        run_pgsql_command(postgres_pod, "DROP TABLE testing3;")

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
