from click.testing import CliRunner

from pg_graphql.cli import main


def test_cli_version():
    runner = CliRunner()
    resp = runner.invoke(main, ["--version"])
    assert resp.exit_code == 0
    assert "version" in resp.output
