import typer

#
import youtube

#
# CLI
#

cli = typer.Typer()
cli.add_typer(youtube.cli, name='youtube')

# @cli.command()
# def deploy():
#     ray.init(address="auto", namespace="serve")

#     @serve.deployment(route_prefix="/")
#     @serve.ingress(api)
#     class FastAPIWrapper:
#         pass

#     FastAPIWrapper.deploy()

#
# Administration
#


# @cli.command()
# def init_db():
#     """Initialize the database"""
#     db.create_indices()

if __name__ == "__main__":
    cli()
