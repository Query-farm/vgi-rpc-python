# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Landing page (``GET {prefix}``) and describe page (``GET {prefix}/describe``)."""

from __future__ import annotations

import html as _html

import falcon

from vgi_rpc.introspect import MethodDescription, parse_describe_batch
from vgi_rpc.rpc import RpcServer

from .._common import _FONT_IMPORTS
from ._responses import _vgi_version

_LANDING_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{protocol_name} &mdash; vgi-rpc</title>
"""
    + _FONT_IMPORTS
    + """
<style>
  body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
         background: #faf8f0; }}
  .logo {{ margin-bottom: 24px; }}
  .logo img {{ width: 140px; height: 140px; border-radius: 50%;
               box-shadow: 0 4px 24px rgba(0,0,0,0.12); }}
  h1 {{ color: #2d5016; margin-bottom: 8px; font-weight: 700; }}
  code {{ font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }}
  a {{ color: #2d5016; text-decoration: none; }}
  a:hover {{ color: #4a7c23; }}
  p {{ line-height: 1.7; color: #6b6b5a; }}
  .links {{ margin-top: 28px; display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; }}
  .links a {{ display: inline-block; padding: 8px 18px; border-radius: 6px;
              border: 1px solid #4a7c23; color: #2d5016; font-weight: 600;
              font-size: 0.9em; transition: all 0.2s ease; }}
  .links a:hover {{ background: #4a7c23; color: #fff; }}
  .links a.primary {{ background: #2d5016; color: #fff; border-color: #2d5016; }}
  .links a.primary:hover {{ background: #4a7c23; border-color: #4a7c23; }}
  footer {{ margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
            color: #6b6b5a; font-size: 0.85em; line-height: 1.8; }}
  footer a {{ color: #2d5016; font-weight: 600; }}
  footer a:hover {{ color: #4a7c23; }}
</style>
</head>
<body>
<div class="logo">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
</div>
<h1>{protocol_name}</h1>
<p>This is a <code>vgi-rpc</code> service endpoint.</p>
<div class="links">
{describe_link}
{repo_link}
<a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
</div>
<footer>
  Powered by <code>vgi-rpc</code> v{version} &middot; server <code>{server_id}</code>
  <br>&copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>"""
)


def _build_landing_html(
    prefix: str,
    protocol_name: str,
    server_id: str,
    describe_path: str | None,
    repo_url: str | None,
) -> bytes:
    """Pre-render the landing page HTML.

    Args:
        prefix: URL prefix for RPC endpoints.
        protocol_name: Protocol name from the ``RpcServer``.
        server_id: Server identity string.
        describe_path: Path to the describe page, or ``None`` to omit the link.
        repo_url: URL to the service's source repository, or ``None``.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    describe_link = (
        f'<a class="primary" href="{_html.escape(describe_path)}">View service API</a>'
        if describe_path is not None
        else ""
    )
    repo_link = f'<a href="{_html.escape(repo_url)}">Source repository</a>' if repo_url else ""
    return _LANDING_HTML_TEMPLATE.format(
        protocol_name=_html.escape(protocol_name),
        server_id=_html.escape(server_id),
        version=_html.escape(_vgi_version()),
        describe_link=describe_link,
        repo_link=repo_link,
    ).encode("utf-8")


class _LandingPageResource:
    """Falcon resource for the landing page at ``GET {prefix}``."""

    __slots__ = ("_body",)

    def __init__(self, body: bytes) -> None:
        self._body = body

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Return the landing page HTML."""
        resp.content_type = "text/html; charset=utf-8"
        resp.set_header("Cache-Control", "no-cache, no-store, must-revalidate, max-age=0")
        resp.data = self._body


_DESCRIBE_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{protocol_name} API Reference &mdash; vgi-rpc</title>
"""
    + _FONT_IMPORTS
    + """
<style>
  body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 900px;
         margin: 0 auto; padding: 40px 20px 0; color: #2c2c1e; background: #faf8f0; }}
  .header {{ text-align: center; margin-bottom: 40px; }}
  .header .logo img {{ width: 80px; height: 80px; border-radius: 50%;
                       box-shadow: 0 3px 16px rgba(0,0,0,0.10); }}
  .header h1 {{ margin-bottom: 4px; color: #2d5016; font-weight: 700; }}
  .header .subtitle {{ color: #6b6b5a; font-size: 1.1em; margin-top: 0; }}
  .header .meta {{ color: #6b6b5a; font-size: 0.9em; }}
  .header .meta a {{ color: #2d5016; font-weight: 600; }}
  .header .meta a:hover {{ color: #4a7c23; }}
  code {{ font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.85em; color: #2c2c1e; }}
  a {{ color: #2d5016; text-decoration: none; }}
  a:hover {{ color: #4a7c23; }}
  .card {{ border: 1px solid #f0ece0; border-radius: 8px; padding: 20px;
           margin-bottom: 16px; background: #fff; }}
  .card:hover {{ border-color: #c8a43a; }}
  .card-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }}
  .method-name {{ font-family: 'JetBrains Mono', monospace; font-size: 1.1em; font-weight: 600;
                  color: #2d5016; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 0.75em; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.03em; }}
  .badge-unary {{ background: #e8f5e0; color: #2d5016; }}
  .badge-stream {{ background: #e0ecf5; color: #1a4a6b; }}
  .badge-exchange {{ background: #f5e6f0; color: #6b234a; }}
  .badge-producer {{ background: #e0f0f5; color: #1a5a6b; }}
  .badge-header {{ background: #f5eee0; color: #6b4423; }}
  .docstring {{ color: #6b6b5a; margin-bottom: 12px; line-height: 1.5; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
  th {{ text-align: left; padding: 8px 10px; background: #f0ece0; color: #2c2c1e;
        font-weight: 600; border-bottom: 2px solid #e0dcd0; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #f0ece0; }}
  td code {{ font-size: 0.85em; }}
  .no-params {{ color: #6b6b5a; font-style: italic; font-size: 0.9em; }}
  .section-label {{ font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                    letter-spacing: 0.05em; color: #6b6b5a; margin-top: 14px;
                    margin-bottom: 6px; }}
  footer {{ text-align: center; margin-top: 48px; padding: 20px 0;
            border-top: 1px solid #f0ece0; color: #6b6b5a; font-size: 0.85em; }}
  footer a {{ color: #2d5016; font-weight: 600; }}
  footer a:hover {{ color: #4a7c23; }}
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
  </div>
  <h1>{protocol_name}</h1>
  <p class="subtitle">API Reference</p>
  <p class="meta">Powered by <code>vgi-rpc</code> v{version} &middot; server <code>{server_id}</code>
{repo_link}</p>
</div>
{method_cards}
<footer>
  <a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
  &middot;
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>"""
)


def _build_method_card(md: MethodDescription) -> str:
    """Build the HTML card for a single RPC method.

    All user-supplied values are HTML-escaped.

    Args:
        md: Parsed method description from introspection.

    Returns:
        An HTML string for the method card.

    """
    badge_cls = "badge-unary" if md.method_type.value == "UNARY" else "badge-stream"
    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_html.escape(md.name)}</span>',
        f'<span class="badge {badge_cls}">{_html.escape(md.method_type.value)}</span>',
    ]
    if md.is_exchange is True:
        parts.append('<span class="badge badge-exchange">exchange</span>')
    elif md.is_exchange is False:
        parts.append('<span class="badge badge-producer">producer</span>')
    if md.has_header:
        parts.append('<span class="badge badge-header">header</span>')
    parts.append("</div>")

    if md.doc:
        parts.append(f'<p class="docstring">{_html.escape(md.doc)}</p>')

    # Parameters table
    if md.param_types:
        parts.append('<div class="section-label">Parameters</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>")
        for pname, ptype in md.param_types.items():
            default_val = md.param_defaults.get(pname)
            default_str = _html.escape(repr(default_val)) if default_val is not None else "&mdash;"
            desc_str = _html.escape(md.param_docs[pname]) if pname in md.param_docs else "&mdash;"
            parts.append(
                f"<tr><td><code>{_html.escape(pname)}</code></td>"
                f"<td><code>{_html.escape(ptype)}</code></td>"
                f"<td>{default_str}</td>"
                f"<td>{desc_str}</td></tr>"
            )
        parts.append("</table>")
    else:
        parts.append('<p class="no-params">No parameters</p>')

    # Returns table for unary methods with return values
    if md.has_return and md.result_schema and len(md.result_schema) > 0:
        parts.append('<div class="section-label">Returns</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        parts.extend(
            f"<tr><td><code>{_html.escape(field.name)}</code></td>"
            f"<td><code>{_html.escape(str(field.type))}</code></td></tr>"
            for field in md.result_schema
        )
        parts.append("</table>")

    # Header schema for stream methods with headers
    if md.has_header and md.header_schema and len(md.header_schema) > 0:
        parts.append('<div class="section-label">Stream Header</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        parts.extend(
            f"<tr><td><code>{_html.escape(field.name)}</code></td>"
            f"<td><code>{_html.escape(str(field.type))}</code></td></tr>"
            for field in md.header_schema
        )
        parts.append("</table>")

    parts.append("</div>")
    return "\n".join(parts)


def _build_describe_html(server: RpcServer, prefix: str, repo_url: str | None) -> bytes:
    """Pre-render the describe page HTML from the server's introspection data.

    Args:
        server: The ``RpcServer`` instance (must have ``describe_enabled``).
        prefix: URL prefix for RPC endpoints.
        repo_url: URL to the service's source repository, or ``None``.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    assert server._describe_batch is not None
    assert server._describe_metadata is not None
    desc = parse_describe_batch(server._describe_batch, server._describe_metadata)

    cards: list[str] = []
    for name in sorted(desc.methods):
        if name == "__describe__":
            continue
        cards.append(_build_method_card(desc.methods[name]))

    repo_link = f'&middot; <a href="{_html.escape(repo_url)}">Source repository</a>' if repo_url else ""
    return _DESCRIBE_HTML_TEMPLATE.format(
        protocol_name=_html.escape(desc.protocol_name),
        server_id=_html.escape(desc.server_id),
        version=_html.escape(_vgi_version()),
        method_cards="\n".join(cards),
        repo_link=repo_link,
    ).encode("utf-8")


class _DescribePageResource:
    """Falcon resource for the describe page at ``GET {prefix}/describe``."""

    __slots__ = ("_body",)

    def __init__(self, body: bytes) -> None:
        self._body = body

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Return the describe page HTML."""
        resp.content_type = "text/html; charset=utf-8"
        resp.set_header("Cache-Control", "no-cache, no-store, must-revalidate, max-age=0")
        resp.data = self._body
