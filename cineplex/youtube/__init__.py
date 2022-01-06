__version__ = '0.1.0'

from bson import ObjectId
from datetime import datetime
import typer
#
import cineplex.youtube.channel as channel


#
# Helpers
#


# def offline_from_file(file: str, ensure_fn, save_fn):
#     """Get items to offline from file"""
#     with open(file, 'r') as infile:
#         offline_items = json.load(infile)

#     id_batch = []
#     id_index = {}
#     for x in offline_items:
#         id = x['id']
#         id_batch.append(id)
#         id_index[id] = datetime.strptime(
#             x['lastUpdated'], '%Y-%m-%dT%H:%M:%S.%fZ')

#     item_with_meta_batch = ensure_fn(id_batch)
#     if not item_with_meta_batch:
#         typer.echo(f"No items to offline")
#         return

#     updated = []
#     for item_with_meta in item_with_meta_batch:
#         id = item_with_meta['_id']
#         last_updated = id_index[id]
#         try:
#             save_fn(id, last_updated)
#             updated.append(id)
#         except Exception as e:
#             typer.echo(
#                 f"💡 {yellow('Error saving offline item')} {green(id)}: {e}")

#     typer.echo(f"✅ {blue(len(updated))} items set for offline")

#
# CLI
#
cli = typer.Typer()
cli.add_typer(channel.cli, name='channel')

if __name__ == "__main__":
    cli()
