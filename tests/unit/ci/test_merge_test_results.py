from __future__ import annotations

import xml.etree.ElementTree as ElementTree
from pathlib import Path

import pytest

from scripts.ci import merge_test_results as merge


def _report(path: Path, *, tests: int, failures: int, name: str) -> Path:
    case = "" if not failures else '<failure message="boom">boom</failure>'
    path.write_text(
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<testsuites><testsuite name="{name}" tests="{tests}" failures="{failures}" '
        f'errors="0" skipped="0">'
        f'<testcase classname="{name}" name="test_one">{case}</testcase>'
        f"</testsuite></testsuites>",
        encoding="utf-8",
    )
    return path


def _totals(path: Path) -> tuple[int, int]:
    """Sum tests and failures the way the quality gate reads them."""
    root = ElementTree.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    tests = sum(int(s.attrib["tests"]) for s in suites)
    failures = sum(int(s.attrib["failures"]) + int(s.attrib["errors"]) for s in suites)
    return tests, failures


class TestMergeTestResults:
    def test_both_lanes_contribute_their_totals(self, tmp_path: Path) -> None:
        fast = _report(tmp_path / "junit-fast.xml", tests=5000, failures=0, name="fast")
        spark = _report(tmp_path / "junit-spark.xml", tests=112, failures=0, name="spark")
        output = tmp_path / "junit.xml"

        assert (
            merge.main(["--junit", str(fast), "--junit", str(spark), "--junit-output", str(output)])
            == 0
        )
        assert _totals(output) == (5112, 0)

    @pytest.mark.parametrize("failing_lane", ["fast", "spark"])
    def test_a_failure_in_either_lane_survives_the_merge(
        self, tmp_path: Path, failing_lane: str
    ) -> None:
        fast = _report(
            tmp_path / "junit-fast.xml",
            tests=5000,
            failures=1 if failing_lane == "fast" else 0,
            name="fast",
        )
        spark = _report(
            tmp_path / "junit-spark.xml",
            tests=112,
            failures=1 if failing_lane == "spark" else 0,
            name="spark",
        )
        output = tmp_path / "junit.xml"

        merge.main(["--junit", str(fast), "--junit", str(spark), "--junit-output", str(output)])

        assert _totals(output) == (5112, 1)

    def test_every_suite_of_a_multi_suite_lane_is_kept(self, tmp_path: Path) -> None:
        """A lane splitting its output into several suites must contribute all of them."""
        lane = tmp_path / "junit-multi.xml"
        lane.write_text(
            '<?xml version="1.0"?><testsuites>'
            '<testsuite name="one" tests="4" failures="0" errors="0" skipped="0"/>'
            '<testsuite name="two" tests="6" failures="1" errors="0" skipped="0"/>'
            "</testsuites>",
            encoding="utf-8",
        )
        other = _report(tmp_path / "junit-other.xml", tests=2, failures=0, name="other")
        output = tmp_path / "junit.xml"

        merge.main(["--junit", str(lane), "--junit", str(other), "--junit-output", str(output)])

        assert _totals(output) == (12, 1)

    def test_a_lane_reporting_no_suite_is_refused(self, tmp_path: Path) -> None:
        empty = tmp_path / "junit-empty.xml"
        empty.write_text('<?xml version="1.0"?><testsuites></testsuites>', encoding="utf-8")

        with pytest.raises(merge.MergeError, match="reports no test suite"):
            merge.merge_junit([empty])

    def test_a_document_that_is_not_junit_is_refused(self, tmp_path: Path) -> None:
        alien = tmp_path / "coverage.xml"
        alien.write_text('<?xml version="1.0"?><coverage line-rate="1"/>', encoding="utf-8")

        with pytest.raises(merge.MergeError, match="not a JUnit report"):
            merge.merge_junit([alien])

    def test_a_missing_lane_report_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(merge.MergeError, match="cannot read JUnit report"):
            merge.merge_junit([tmp_path / "absent.xml"])

    def test_a_single_suite_document_is_accepted(self, tmp_path: Path) -> None:
        solo = tmp_path / "junit-solo.xml"
        solo.write_text(
            '<?xml version="1.0"?><testsuite name="solo" tests="3" failures="1" errors="0" '
            'skipped="0"><testcase classname="solo" name="t"/></testsuite>',
            encoding="utf-8",
        )
        output = tmp_path / "junit.xml"

        merge.main(["--junit", str(solo), "--junit-output", str(output)])

        assert _totals(output) == (3, 1)
