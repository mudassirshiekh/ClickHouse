import argparse
import os
import subprocess
import time
from pathlib import Path

from praktika.result import Result
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_version import CHVersion


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    INSTALL_CLICKHOUSE_REFERENCE = "install_reference"
    DOWNLOAD_DATASETS = "download"
    CONFIGURE = "configure"
    RESTART = "restart"
    TEST = "test"


class CHServer:
    # upstream/master
    LEFT_SERVER_PORT = 9001
    LEFT_SERVER_KEEPER_PORT = 9181
    LEFT_SERVER_KEEPER_RAFT_PORT = 9234
    LEFT_SERVER_INTERSERVER_PORT = 9009
    # patched version
    RIGHT_SERVER_PORT = 19001
    RIGHT_SERVER_KEEPER_PORT = 19181
    RIGHT_SERVER_KEEPER_RAFT_PORT = 19234
    RIGHT_SERVER_INTERSERVER_PORT = 19009

    def __init__(self, is_left=False):
        if is_left:
            server_port = self.LEFT_SERVER_PORT
            keeper_port = self.LEFT_SERVER_KEEPER_PORT
            raft_port = self.LEFT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.LEFT_SERVER_INTERSERVER_PORT
            serever_path = "/tmp/praktika/perf_wd/left"
            log_file = f"{serever_path}/server.log"
        else:
            server_port = self.RIGHT_SERVER_PORT
            keeper_port = self.RIGHT_SERVER_KEEPER_PORT
            raft_port = self.RIGHT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.RIGHT_SERVER_INTERSERVER_PORT
            serever_path = "/tmp/praktika/perf_wd/right"
            log_file = f"{serever_path}/server.log"

        self.preconfig_start_cmd = f"{serever_path}/clickhouse-server --config-file={serever_path}/config/config.xml -- --path /tmp/praktika/db0 --user_files_path /tmp/praktika/db0/user_files --top_level_domains_path /tmp/praktika/perf_wd/top_level_domains --keeper_server.storage_path /tmp/praktika/coordination0 --tcp_port {server_port}"
        self.log_fd = None
        self.log_file = log_file
        self.port = server_port
        self.server_path = serever_path
        self.name = "Reference" if is_left else "Patched"

        self.start_cmd = f"{serever_path}/clickhouse-server --config-file={serever_path}/config/config.xml \
            -- --path {serever_path}/db --user_files_path {serever_path}/db/user_files \
            --top_level_domains_path {serever_path}/top_level_domains --tcp_port {server_port} \
            --keeper_server.tcp_port {keeper_port} --keeper_server.raft_configuration.server.port {raft_port} \
            --keeper_server.storage_path {serever_path}/coordination --zookeeper.node.port {keeper_port} \
            --interserver_http_port {inter_server_port}"

    def start_preconfig(self):
        print("Starting ClickHouse server")
        print("Command: ", self.preconfig_start_cmd)
        self.log_fd = open(self.log_file, "w")
        self.proc = subprocess.Popen(
            self.preconfig_start_cmd,
            stderr=subprocess.STDOUT,
            stdout=self.log_fd,
            shell=True,
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print(f"ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print(f"ClickHouse server ready")
        else:
            print(f"ClickHouse server NOT ready")

        res = res and Shell.check(
            f"clickhouse-client --port {self.port} --query 'create database IF NOT EXISTS test'",
            verbose=True,
        )
        # res = res and Shell.check(f"clickhouse-client --port {self.port} --query 'rename table datasets.hits_v1 to test.hits'", verbose=True)
        return res

    def start(self):
        print(f"Starting [{self.name}] ClickHouse server")
        print("Command: ", self.start_cmd)
        self.log_fd = open(self.log_file, "w")
        self.proc = subprocess.Popen(
            self.start_cmd, stderr=subprocess.STDOUT, stdout=self.log_fd, shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print(f"ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print(f"ClickHouse server ready")
        else:
            print(f"ClickHouse server NOT ready")
        return res

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {self.port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True

    def ask(self, query):
        return Shell.get_output(
            f'{self.server_path}/clickhouse-client --port {self.port} --query "{query}"'
        )

    @classmethod
    def run_test(cls, test_file, runs=5):
        results_path = "/tmp/praktika/perf_wd/test_results"
        Shell.check(f"rm -rf {results_path} && mkdir -p {results_path}")
        test_name = test_file.split('/')[-1].removesuffix('.xml')
        res, out, err = Shell.get_res_stdout_stderr(
            f"TIMEFORMAT=$(printf \"{test_name}\t%%3R\t%%3U\t%%3S\n\"); \
            ( time ./tests/performance/scripts/perf.py --host localhost localhost \
                --port {cls.LEFT_SERVER_PORT} {cls.RIGHT_SERVER_PORT} \
                --runs {runs} --max-queries 0 \
                --profile-seconds 0 \
                {test_file} >{results_path}/{test_name}-raw.tsv 2>{results_path}/{test_name}-err.log \
            ) 2>>{results_path}/wall-clock-times.tsv >/dev/null \
                || echo \"Test {test_name} failed with error code $?\" >> {results_path}/{test_name}-err.log",
            verbose=True
        )
        if res != 0:
            print(f"Test [{test_name}] run failed, err [{err}], out [{out}]")
        # TODO:
        # (
        # time "$script_dir/perf.py" "${argv[@]}" > "$test_name-raw.tsv" 2> "$test_name-err.log"
        # ) 2>>wall-clock-times.tsv >/dev/null \
        #         || echo "Test $test_name failed with error code $?" >> "$test_name-err.log"
        # ) 2>/dev/null

    def terminate(self):
        print("Terminate ClickHouse process")
        timeout = 10
        if self.proc:
            Utils.terminate_process_group(self.proc.pid)

            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
                print(f"Process {self.proc.pid} terminated gracefully.")
            except Exception:
                print(
                    f"Process {self.proc.pid} did not terminate in {timeout} seconds, killing it..."
                )
                Utils.terminate_process_group(self.proc.pid, force=True)
                self.proc.wait()  # Wait for the process to be fully killed
                print(f"Process {self.proc} was killed.")
        if self.log_fd:
            self.log_fd.close()


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Performance Tests Job")
    parser.add_argument(
        "--ch-path", help="Path to clickhouse binary", default=f"/tmp/praktika/input"
    )
    parser.add_argument(
        "--test-options",
        help="Comma separated option(s) BATCH_NUM/BTATCH_TOT|?",
        default="",
    )
    parser.add_argument("--param", help="Optional job start stage", default=None)
    parser.add_argument("--test", help="Optional test name pattern", default="")
    return parser.parse_args()


def main():

    args = parse_args()
    test_options = args.test_options.split(",")
    batch_num, total_batches = 0, 0
    build_type = ""
    build_name = ""
    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        if "amd" in to or "arm" in to:
            build_type = to

    if Utils.is_arm():
        build_type = "arm_release"
        build_name = "package_aarch64"
    elif Utils.is_amd():
        build_type = "amd_release"
        build_name = "package_release"
    else:
        Utils.raise_with_error(f"Unknown processor architecture")

    test_keyword = args.test

    ch_path = args.ch_path
    assert (
        Path(ch_path + "/clickhouse").is_file()
        or Path(ch_path + "/clickhouse").is_symlink()
    ), f"clickhouse binary not found under [{ch_path}]"

    stop_watch = Utils.Stopwatch()
    stages = list(JobStages)

    logs_to_attach = []

    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    res = True
    perf_wd = "/tmp/praktika/perf_wd"
    perf_right = f"{perf_wd}/right/"
    perf_left = f"{perf_wd}/left/"
    perf_right_config = f"{perf_right}/config"
    perf_left_config = f"{perf_left}/config"
    results = []

    # Shell.check(f"rm -rf {perf_wd} && mkdir -p {perf_wd}")

    # add right CH location to PATH
    Utils.add_to_PATH(perf_right)

    # TODO:
    # Set python output encoding so that we can print queries with non-ASCII letters.
    # export PYTHONIOENCODING=utf-8
    # script_path="tests/performance/scripts/"
    # ulimit -c unlimited
    # cat /proc/sys/kernel/core_pattern

    if res and JobStages.INSTALL_CLICKHOUSE in stages:
        print("Install ClickHouse")
        commands = [
            f"mkdir -p {perf_right_config}",
            f"cp ./programs/server/config.xml {perf_right_config}",
            f"cp ./programs/server/users.xml {perf_right_config}",
            f"cp -r --dereference ./programs/server/config.d {perf_right_config}",
            f"cp -r ./tests/config/top_level_domains {perf_wd}",
            f"cp -r ./tests/performance {perf_right}",
            f"chmod +x {ch_path}/clickhouse",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-keeper",
        ]
        results.append(
            Result.from_commands_run(
                name="Install ClickHouse", command=commands, with_log=True
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.INSTALL_CLICKHOUSE_REFERENCE in stages:
        print("Install Reference")
        if not Path(f"{perf_left}/.done").is_file():
            left_major, left_minor, left_sha = (
                CHVersion.get_latest_release_major_minor_sha()
            )
            # TODO: use config from the same sha as reference CH binary
            # git checkout left_sha
            # rm -rf /tmp/praktika/left && mkdir -p /tmp/praktika/left
            # cp -r ./tests/config /tmp/praktika/left/config
            # git checkout -
            commands = [
                f"mkdir -p {perf_left_config}",
                f"wget -nv -P {perf_left}/ https://clickhouse-builds.s3.us-east-1.amazonaws.com/{left_major}.{left_minor-1}/{left_sha}/{build_name}/clickhouse",
                f"chmod +x {perf_left}/clickhouse",
                f"cp -r ./tests/performance /tmp/praktika/left/",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-local",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-client",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-server",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-keeper",
            ]
            results.append(
                Result.from_commands_run(
                    name="Install Reference ClickHouse", command=commands, with_log=True
                )
            )
            res = results[-1].is_ok()
            Shell.check(f"touch {perf_left}/.done")

    if res and JobStages.DOWNLOAD_DATASETS in stages:
        print("Download datasets")
        db_path = "/tmp/praktika/db0/"

        if not Path(f"{db_path}/.done").is_file():
            Shell.check(f"mkdir -p {db_path}", verbose=True)
            datasets = ["hits1", "hits10", "hits100", "values"]
            dataset_paths = {
                "hits10": "https://clickhouse-private-datasets.s3.amazonaws.com/hits_10m_single/partitions/hits_10m_single.tar",
                "hits100": "https://clickhouse-private-datasets.s3.amazonaws.com/hits_100m_single/partitions/hits_100m_single.tar",
                "hits1": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
                "values": "https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar",
            }
            cmds = []
            for dataset_path in dataset_paths.values():
                cmds.append(
                    f'wget -nv -nd -c "{dataset_path}" -O- | tar --extract --verbose -C {db_path}'
                )
            res = Shell.check_parallel(cmds, verbose=True)
            if res:
                Shell.check(f"touch {db_path}/.done")

    if res and JobStages.CONFIGURE in stages:
        print("Configure")

        leftCH = CHServer(is_left=True)

        def restart_ch():
            res_ = leftCH.start_preconfig()
            leftCH.terminate()
            return res_

        commands = [
            f"echo \"ATTACH DATABASE default ENGINE=Ordinary\" > {db_path}/metadata/default.sql",
            f"echo \"ATTACH DATABASE datasets ENGINE=Ordinary\" > {db_path}/metadata/datasets.sql",
            f"ls {db_path}/metadata",
            f"rm {perf_right_config}/config.d/text_log.xml ||:",
            # backups disk uses absolute path, and this overlaps between servers, that could lead to errors
            f"rm {perf_right_config}/config.d/backups.xml ||:",
            f"cp -rv {perf_right_config} {perf_left}/",
            restart_ch,
            # Make copies of the original db for both servers. Use hardlinks instead
            # of copying to save space. Before that, remove preprocessed configs and
            # system tables, because sharing them between servers with hardlinks may
            # lead to weird effects
            f"rm -rf {perf_left}/db",
            f"rm -rf {perf_right}/db",
            f"rm -r {db_path}/preprocessed_configs",
            f"rm -r {db_path}/data/system",
            f"rm -r {db_path}/metadata/system",
            f"rm -r {db_path}/status",
            f"cp -al {db_path} {perf_left}/db",
            f"cp -al {db_path} {perf_right}/db",
            f"cp -R /tmp/praktika/coordination0 {perf_left}/coordination",
            f"cp -R /tmp/praktika/coordination0 {perf_right}/coordination",
        ]
        results.append(
            Result.from_commands_run(name="Configure", command=commands, with_log=True)
        )
        res = results[-1].is_ok()

    leftCH = CHServer(is_left=True)
    rightCH = CHServer(is_left=False)
    if res and JobStages.RESTART in stages:
        print("Start")

        def restart_ch1():
            res_ = leftCH.start()
            return res_

        def restart_ch2():
            res_ = rightCH.start()
            return res_

        commands = [
            restart_ch1,
            restart_ch2,
        ]
        results.append(
            Result.from_commands_run(name="Start", command=commands, with_log=True)
        )
        # TODO : check datasets are loaded:
        print(
            leftCH.ask(
                "select * from system.tables where database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
            )
        )
        print(leftCH.ask("select * from system.build_options"))
        print(
            rightCH.ask(
                "select * from system.tables where database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
            )
        )
        print(rightCH.ask("select * from system.build_options"))
        res = results[-1].is_ok()

    if res and JobStages.TEST in stages:
        print("Tests")
        test_files = [file for file in os.listdir('./tests/performance/') if file.endswith('.xml')]
        if test_keyword:
            test_files = [file for file in test_files if test_keyword in file]
        print(f"Test Files: [{test_files}]")
        assert test_files
        for test in test_files:
            CHServer.run_test('./tests/performance/' + test)
            #time.sleep(300)
        # commands = [
        #     "./tests/performance/scripts/perf.py --help > /dev/null",
        # ]
        # results.append(
        #     Result.from_commands_run(name="Tests", command=commands, with_log=True)
        # )
        # res = results[-1].is_ok()


# function run_tests
# {
#     # Just check that the script runs at all
#     "$script_dir/perf.py" --help > /dev/null
#
# # Find the directory with test files.
# if [ -v CHPC_TEST_PATH ]
# then
# # Use the explicitly set path to directory with test files.
# test_prefix="$CHPC_TEST_PATH"
# elif [ "$PR_TO_TEST" == "0" ]
# then
# # When testing commits from master, use the older test files. This
# # allows the tests to pass even when we add new functions and tests for
# # them, that are not supported in the old revision.
# test_prefix=left/performance
# else
# # For PRs, use newer test files so we can test these changes.
# test_prefix=right/performance
# fi
#
# run_only_changed_tests=0
#
# # Determine which tests to run.
# if [ -v CHPC_TEST_GREP ]
# then
# # Run only explicitly specified tests, if any.
# # shellcheck disable=SC2010
# test_files=($(ls "$test_prefix" | rg "$CHPC_TEST_GREP" | xargs -I{} -n1 readlink -f "$test_prefix/{}"))
# elif [ "$PR_TO_TEST" -ne 0 ] \
#      && [ "$(wc -l < changed-test-definitions.txt)" -gt 0 ] \
#      && [ "$(wc -l < other-changed-files.txt)" -eq 0 ]
# then
# # If only the perf tests were changed in the PR, we will run only these
# # tests. The lists of changed files are prepared in entrypoint.sh because
# # it has the repository.
# test_files=($(sed "s/tests\/performance/${test_prefix//\//\\/}/" changed-test-definitions.txt))
# run_only_changed_tests=1
# else
# # The default -- run all tests found in the test dir.
# test_files=($(ls "$test_prefix"/*.xml))
# fi
#
# # We can filter out certain tests
# if [ -v CHPC_TEST_GREP_EXCLUDE ]; then
# # filter tests array in bash https://stackoverflow.com/a/40375567
# filtered_test_files=( $( for i in ${test_files[@]} ; do echo $i ; done | rg -v ${CHPC_TEST_GREP_EXCLUDE} ) )
# test_files=("${filtered_test_files[@]}")
# fi
#
# # We split perf tests into multiple checks to make them faster
# if [ -v CHPC_TEST_RUN_BY_HASH_TOTAL ]; then
# # filter tests array in bash https://stackoverflow.com/a/40375567
# for index in "${!test_files[@]}"; do
# [ $(( index % CHPC_TEST_RUN_BY_HASH_TOTAL )) != "$CHPC_TEST_RUN_BY_HASH_NUM" ] && \
#         unset -v 'test_files[$index]'
# done
# # to have sequential indexes...
# test_files=("${test_files[@]}")
# fi
#
# if [ "$run_only_changed_tests" -ne 0 ]; then
# if [ ${#test_files[@]} -eq 0 ]; then
# time "$script_dir/report.py" --no-tests-run > report.html
# exit 0
# fi
# fi
#
# # For PRs w/o changes in test definitions, test only a subset of queries,
# # and run them less times. If the corresponding environment variables are
# # already set, keep those values.
# #
# # NOTE: too high CHPC_RUNS/CHPC_MAX_QUERIES may hit internal CI timeout.
# # NOTE: Currently we disabled complete run even for master branch
# #if [ "$PR_TO_TEST" -ne 0 ] && [ "$(wc -l < changed-test-definitions.txt)" -eq 0 ]
# #then
# #    CHPC_RUNS=${CHPC_RUNS:-7}
# #    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-10}
# #else
# #    CHPC_RUNS=${CHPC_RUNS:-13}
# #    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-0}
# #fi
#
# CHPC_RUNS=${CHPC_RUNS:-7}
# CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-10}
#
# export CHPC_RUNS
# export CHPC_MAX_QUERIES
#
# # Determine which concurrent benchmarks to run. For now, the only test
# # we run as a concurrent benchmark is 'website'. Run it as benchmark if we
# # are also going to run it as a normal test.
# for test in ${test_files[@]}; do echo "$test"; done | sed -n '/website/p' > benchmarks-to-run.txt
#
# # Delete old report files.
# for x in {test-times,wall-clock-times}.tsv
# do
# rm -v "$x" ||:
#     touch "$x"
# done
#
# # Randomize test order. BTW, it's not an array no more.
# test_files=$(for f in ${test_files[@]}; do echo "$f"; done | sort -R)
#
# # Limit profiling time to 10 minutes, not to run for too long.
# profile_seconds_left=600
#
# # Run the tests.
# total_tests=$(echo "$test_files" | wc -w)
# current_test=0
# test_name="<none>"
# for test in $test_files
# do
# echo "$current_test of $total_tests tests complete" > status.txt
# # Check that both servers are alive, and restart them if they die.
# clickhouse-client --port $LEFT_SERVER_PORT --query "select 1 format Null" \
#         || { echo $test_name >> left-server-died.log ; restart ; }
# clickhouse-client --port $RIGHT_SERVER_PORT --query "select 1 format Null" \
#         || { echo $test_name >> right-server-died.log ; restart ; }
#
# test_name=$(basename "$test" ".xml")
# echo test "$test_name"

# # Don't profile if we're past the time limit.
# # Use awk because bash doesn't support floating point arithmetic.
# profile_seconds=$(awk "BEGIN { print ($profile_seconds_left > 0 ? 10 : 0) }")
#
# if rg --quiet "$(basename $test)" changed-test-definitions.txt
# then
# # Run all queries from changed test files to ensure that all new queries will be tested.
# max_queries=0
# else
# max_queries=$CHPC_MAX_QUERIES
# fi
#
# (
# set +x
# argv=(
# --host localhost localhost
# --port "$LEFT_SERVER_PORT" "$RIGHT_SERVER_PORT"
# --runs "$CHPC_RUNS"
# --max-queries "$max_queries"
# --profile-seconds "$profile_seconds"
#
# "$test"
# )
# TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
# # one more subshell to suppress trace output for "set +x"
# (
# time "$script_dir/perf.py" "${argv[@]}" > "$test_name-raw.tsv" 2> "$test_name-err.log"
# ) 2>>wall-clock-times.tsv >/dev/null \
#         || echo "Test $test_name failed with error code $?" >> "$test_name-err.log"
# ) 2>/dev/null
#
# profile_seconds_left=$(awk -F'	' \
#         'BEGIN { s = '$profile_seconds_left'; } /^profile-total/ { s -= $2 } END { print s }' \
#         "$test_name-raw.tsv")
# current_test=$((current_test + 1))
# done
#
# wait
# }

    # if res and JobStages.TEST in stages:
    #     print("Tests")
    #     commands = [
    #         "stage=restart ./ci/jobs/scripts/perf/compare.sh",
    #     ]
    #     results.append(
    #         Result.from_commands_run(name="Tests", command=commands, with_log=True)
    #     )
    #     res = results[-1].is_ok()

    # TODO:
    # Start the main comparison script.
    # {
    #     time ./ci/jobs/scripts/perf/download.sh "$REF_PR" "$REF_SHA" "$PR_TO_TEST" "$SHA_TO_TEST" && \
    #                                             time stage=configure ./ci/jobs/scripts/perf/compare.sh ; \
    #     } 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee -a compare.log

    # Stop the servers to free memory. Normally they are restarted before getting
    # the profile info, so they shouldn't use much, but if the comparison script
    # fails in the middle, this might not be the case.
    # for _ in {1..30}
    # do
    # pkill clickhouse || break
    # sleep 1
    # done

    # dmesg -T > dmesg.log
    #
    # ls -lath
    #
    # 7z a '-x!*/tmp' /output/output.7z ./*.{log,tsv,html,txt,rep,svg,columns} \
    #    {right,left}/{performance,scripts} {{right,left}/db,db0}/preprocessed_configs \
    #    report analyze benchmark metrics \
    #    ./*.core.dmp ./*.core

    ## If the files aren't same, copy it
    # cmp --silent compare.log /tmp/praktika/compare.log || \
    #  cp compare.log /output

    Result.create_from(
        results=results, stopwatch=stop_watch, files=logs_to_attach if not res else []
    ).complete_job()


if __name__ == "__main__":
    main()
