from latch.types import LatchAuthor, LatchMetadata, LatchParameter

LONGQC_DOCS = LatchMetadata(
    display_name="LongQC",
    documentation="https://github.com/jvfe/longqc_latch/blob/main/README.md",
    author=LatchAuthor(
        name="jvfe",
        github="https://github.com/jvfe",
    ),
    repository="https://github.com/jvfe/longqc_latch",
    license="MIT",
)

LONGQC_DOCS.parameters = {
    "read": LatchParameter(
        display_name="Reads",
        description="Single-end long-read file",
        section_title="Data",
    ),
    "sample_name": LatchParameter(
        display_name="Sample name",
        description="Sample name (will define output file names)",
    ),
    "min_mean_q": LatchParameter(
        display_name="Minimum mean quality threshold.",
        section_title="FiltLong parameters",
    ),
    "keep_percent": LatchParameter(
        display_name="Best read percentage to keep",
        description="keep only this percentage of the best reads (measured by bases)",
    ),
    "min_length": LatchParameter(
        display_name="Minimum length threshold.",
    ),
    "max_length": LatchParameter(
        display_name="Maximum length threshold.",
    ),
    "min_window_q": LatchParameter(
        display_name="Minimum window quality threshold.",
    ),
}
