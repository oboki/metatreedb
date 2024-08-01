from metatree.util import resolve_file_url
from os import environ


def test_resolve_file_url(request, spam="eggs"):
    assert resolve_file_url(f"./.{spam}") == f"file://{request.config.rootdir}/.{spam}"
    assert resolve_file_url(f"$HOME/.{spam}") == f"file://{environ['HOME']}/.{spam}"
    assert (
        resolve_file_url(f"file://./.{spam}")
        == f"file://{request.config.rootdir}/.{spam}"
    )
    assert (
        resolve_file_url(f"file://$HOME/.{spam}") == f"file://{environ['HOME']}/.{spam}"
    )
