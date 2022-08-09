"""
Long read preprocessing and quality control
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Union

from latch import message, small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchDir, LatchFile

from .docs import LONGQC_DOCS


@small_task
def nanoplot(read: LatchFile, sample_name: str) -> LatchDir:

    output_name = f"{sample_name}_prefilt"
    if "trim" in read.local_path:
        output_name = f"{sample_name}_postfilt"

    output_dir = Path(output_name).resolve()

    _nanoplot_cmd = [
        "NanoPlot",
        "--fastq",
        read.local_path,
        "--tsv_stats",
        "--no_static",
        "--info_in_report",
        "-o",
        output_name,
    ]

    try:
        subprocess.run(_nanoplot_cmd)
    except subprocess.CalledProcessError as e:
        message(
            "error",
            {
                "title": f"NanoPlot error for {output_name}",
                "body": f"Error surfaced:\n{str(e)}",
            },
        )
        raise RuntimeError(f"NanoPlot error: {e}")

    return LatchDir(str(output_dir), f"latch:///longqc/{sample_name}/{output_name}")


@small_task
def run_filtlong(
    read: LatchFile,
    sample_name: str,
    min_mean_q: float,
    keep_percent: float,
    min_length: Optional[int],
    max_length: Optional[int],
    min_window_q: Optional[float],
) -> LatchFile:
    output_filename = f"{sample_name}_trim_final.fastq"
    output_file = Path(output_filename).resolve()

    _filtlong_cmd = [
        "filtlong",
    ]

    if keep_percent is not None:
        _filtlong_cmd.extend(
            [
                "--keep_percent",
                str(keep_percent),
            ]
        )
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
                str(min_length),
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

    _filtlong_cmd.extend([read.local_path])

    message(
        "info",
        {"title": "Filtlong command", "body": f"Running {' '.join(_filtlong_cmd)}"},
    )

    try:
        with open(output_file, "wb") as output:
            subprocess.run(_filtlong_cmd, check=True, stdout=output)
    except subprocess.CalledProcessError as e:
        message(
            "error",
            {
                "title": f"FiltLong error for {sample_name}",
                "body": f"Error surfaced:\n{str(e)}",
            },
        )
        raise RuntimeError(f"FiltLong error: {e}")

    return LatchFile(
        str(output_file), f"latch:///longqc/{sample_name}/{output_filename}"
    )


@small_task
def run_porechop(read: LatchFile, sample_name: str) -> LatchFile:

    output_filename = f"{sample_name}_trim.fastq"
    output_file = Path(output_filename).resolve()

    _porechop_cmd = ["porechop", "-i", read.local_path, "-o", str(output_file)]

    message(
        "info",
        {"title": "Porechop command", "body": f"Running {' '.join(_porechop_cmd)}"},
    )

    try:
        subprocess.run(_porechop_cmd)

    except subprocess.CalledProcessError as e:
        message(
            "error",
            {
                "title": f"Porechop error for {sample_name}",
                "body": f"Error surfaced:\n{str(e)}",
            },
        )
        raise RuntimeError(f"Porechop error: {e}")

    return LatchFile(
        str(output_file), f"latch:///longqc/{sample_name}/{output_filename}"
    )


@workflow(LONGQC_DOCS)
def longqc(
    read: LatchFile,
    sample_name: str,
    min_mean_q: float = 25.0,
    keep_percent: float = 90.0,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    min_window_q: Optional[float] = None,
) -> List[Union[LatchDir, LatchFile]]:
    """Quality control and preprocessing for long-read data

    LongQC
    ----

    A Workflow for long read preprocessing and quality control. It's composed of:

    - [NanoPlot](https://github.com/wdecoster/NanoPlot) [^1] for creating visualizations of read quality before **and**
        after processing.
    - [PoreChop](https://github.com/rrwick/Porechop) for trimming sequence adapters
    - [FiltLong](https://github.com/rrwick/Filtlong) for low-quality filtering and other preprocessing.


    [^1]: Wouter De Coster, Svenn D'Hert, Darrin T Schultz, Marc Cruts,
    Christine Van Broeckhoven,
    NanoPack: visualizing and processing long-read sequencing data,
    Bioinformatics, Volume 34, Issue 15, 01 August 2018, Pages 2666–2669,
    https://doi.org/10.1093/bioinformatics/bty149
    """
    prefilt = nanoplot(read=read, sample_name=sample_name)
    trimmed = run_porechop(
        read=read,
        sample_name=sample_name,
    )
    final_trimmed = run_filtlong(
        read=trimmed,
        sample_name=sample_name,
        min_length=min_length,
        max_length=max_length,
        min_mean_q=min_mean_q,
        min_window_q=min_window_q,
        keep_percent=keep_percent,
    )
    postfilt = nanoplot(read=final_trimmed, sample_name=sample_name)

    return [prefilt, final_trimmed, postfilt]


LaunchPlan(
    longqc,
    "MinION IBS Microbiome (SRR19142056)",
    {
        "read": LatchFile("s3://latch-public/test-data/4318/SRR19142056.fastq"),
        "sample_name": "SRR19142056",
        "min_length": None,
        "max_length": None,
        "min_window_q": None,
        "min_mean_q": 25.0,
        "keep_percent": 90.0,
    },
)
