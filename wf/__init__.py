"""
Long read preprocessing and quality control
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Union

from latch import message, small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchDir, LatchFile


@small_task
def nanoplot(read: LatchFile, output_name: str) -> LatchDir:

    output_dir = Path(output_name).resolve()

    _nanoplot_cmd = [
        "NanoPlot",
        "--fastq",
        read.local_path,
        "--tsv_stats",
        "--info_in_report",
        "--no_static",
        "-o",
        output_name,
    ]

    try:
        subprocess.run(_nanoplot_cmd, capture_output=True)
    except subprocess.CalledProcessError as e:
        message(
            "error",
            {
                "title": f"NanoPlot error for {output_name}",
                "body": f"Error surfaced:\n{str(e)}",
            },
        )
        raise RuntimeError(f"NanoPlot error: {e}")

    return LatchDir(str(output_dir), f"latch:///longqc/nanoplot/{output_name}")


@small_task
def run_filtlong(
    read: LatchFile,
    sample_name: str,
    min_length: Optional[int],
    max_length: Optional[int],
    min_mean_q: Optional[float],
    min_window_q: Optional[float],
) -> LatchFile:
    output_filename = f"{sample_name}_trim.fastq"
    output_file = Path(output_filename).resolve()

    _filtlong_cmd = [
        "filtlong",
    ]

    if min_mean_q is not None:
        _filtlong_cmd.extend(
            [
                "--min_mean_q",
                str(min_mean_q),
            ]
        )
    if min_length is not None:
        _filtlong_cmd.extend(
            [
                "--min_length",
                str(min_mean_q),
            ]
        )
    if max_length is not None:
        _filtlong_cmd.extend(
            [
                "--max_length",
                str(max_length),
            ]
        )
    if min_window_q is not None:
        _filtlong_cmd.extend(
            [
                "--min_window_q",
                str(min_window_q),
            ]
        )

    output_str = [read.local_path, ">", output_filename]

    _filtlong_cmd.extend(output_str)

    try:
        subprocess.run(_filtlong_cmd)
    except subprocess.CalledProcessError as e:
        message(
            "error",
            {
                "title": f"FiltLong error for {sample_name}",
                "body": f"Error surfaced:\n{str(e)}",
            },
        )
        raise RuntimeError(f"NanoPlot error: {e}")

    return LatchFile(str(output_file), f"latch:///longqc/trimmed/{output_filename}")


@workflow
def longqc(
    read: LatchFile,
    sample_name: str,
    min_mean_q: float = 25.0,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    min_window_q: Optional[float] = None,
) -> List[Union[LatchDir, LatchFile]]:
    """Quality control and preprocessing for long-read data

    LongQC
    ----

    __metadata__:
        display_name: LongQC
        author:
            name:
            email:
            github:
        repository:
        license:
            id: MIT

    Args:

        read:
          Paired-end read 1 file to be assembled.

          __metadata__:
            display_name: Read
            section_title: Data

        sample_name:
          Sample name (Will defined output file names).

          __metadata__:
            display_name: Sample name

        min_length:
          Minimum length threshold.

          __metadata__:
            display_name: Minimum length threshold
            section_title: FiltLong parameters

        max_length:
          Maximum length threshold.

          __metadata__:
            display_name: Maximum length threshold

        min_mean_q:
          Minimum mean quality threshold.

          __metadata__:
            display_name: Minimum mean quality threshold

        min_window_q:
          Minimum window quality threshold.

          __metadata__:
            display_name: Minimum window quality threshold
    """
    prefilt = nanoplot(read=read, output_name=f"{sample_name}_prefilt")
    trimmed = run_filtlong(
        read=read,
        sample_name=sample_name,
        min_length=min_length,
        max_length=max_length,
        min_mean_q=min_mean_q,
        min_window_q=min_window_q,
    )
    postfilt = nanoplot(read=trimmed, output_name=f"{sample_name}_postfilt")

    return [prefilt, trimmed, postfilt]


LaunchPlan(
    longqc,
    "MinION IBS Microbiome",
    {
        "read": LatchFile("s3://latch-public/test-data/4318/SRR19142056.fastq"),
        "sample_name": "SRR19142056",
        "min_length": None,
        "max_length": None,
        "min_window_q": None,
        "min_mean_q": 25.0,
    },
)
