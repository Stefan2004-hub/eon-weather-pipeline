from __future__ import annotations

from weather_pipeline.cli import main


def test_cli_help_exits_successfully(capsys) -> None:
    try:
        main(["--help"])
    except SystemExit as exc:
        assert exc.code == 0

    assert "Run the E.ON weather intelligence pipeline" in capsys.readouterr().out


def test_cli_requires_a_command(capsys) -> None:
    try:
        main([])
    except SystemExit as exc:
        assert exc.code == 2

    assert "the following arguments are required: command" in capsys.readouterr().err
