import json
import re
import uuid
from cog.config import D3_CDN


def _theme_js(dark=False):
    """
    Return a ``var cogTheme = { ... };`` block that defines every
    colour used by the D3 visualisation.
    """
    if dark:
        return """var cogTheme = {
  bg: "#0f172a",
  tooltipBg: "rgba(15,23,42,0.92)",
  tooltipBorder: "1px solid rgba(148,163,184,0.25)",
  tooltipColor: "#e2e8f0",
  tooltipShadow: "0 4px 14px rgba(0,0,0,0.45)",
  tooltipDetail: "#94a3b8",
  arrow: "#94a3b8",
  edge: "#94a3b8",
  edgeLabel: "#94a3b8",
  nodeLabel: "#e2e8f0",
  textShadow: "0 0 4px rgba(0,0,0,0.6)"
};"""
    else:
        return """var cogTheme = {
  bg: "#ffffff",
  tooltipBg: "rgba(30,30,30,0.92)",
  tooltipBorder: "1px solid rgba(0,0,0,0.15)",
  tooltipColor: "#f0f0f0",
  tooltipShadow: "0 4px 14px rgba(0,0,0,0.25)",
  tooltipDetail: "#ccc",
  arrow: "#6b7280",
  edge: "#6b7280",
  edgeLabel: "#6b7280",
  nodeLabel: "#1f2937",
  textShadow: "none"
};"""


def _d3_graph_js():
    """
    Returns the standalone D3 visualization JavaScript.

    Expects three globals to be defined before this script runs:
      - ``graphData``  – {nodes: [{id}], links: [{source, target, label}]}
      - ``cogContainer`` – the DOM element to render into
      - ``cogTheme`` – colour palette object (from ``_theme_js``)
    """
    return r"""
(function() {
  var container = cogContainer;
  var data = graphData;
  var T = cogTheme;
  if (!data || !data.nodes || !data.links) return;

  var width  = container.clientWidth  || 700;
  var height = container.clientHeight || 500;

  /* --- Nodes & links (shallow copy so D3 can mutate) --- */
  var nodes = data.nodes.map(function(n) { return {id: n.id}; });
  var links = data.links.map(function(l) {
    return {source: l.source, target: l.target, label: l.label || ""};
  });

  /* --- Tooltip --- */
  var tooltip = d3.select(container)
    .append("div")
    .style("position", "absolute")
    .style("background", T.tooltipBg)
    .style("border", T.tooltipBorder)
    .style("border-radius", "8px")
    .style("padding", "8px 12px")
    .style("font-size", "11px")
    .style("color", T.tooltipColor)
    .style("pointer-events", "none")
    .style("opacity", "0")
    .style("transition", "opacity 0.15s")
    .style("z-index", "10")
    .style("max-width", "220px")
    .style("box-shadow", T.tooltipShadow)
    .style("font-family", "system-ui, -apple-system, sans-serif");

  /* --- SVG --- */
  var svg = d3.select(container)
    .append("svg")
    .attr("width", "100%")
    .attr("height", "100%")
    .attr("viewBox", "0 0 " + width + " " + height)
    .attr("preserveAspectRatio", "xMidYMid meet")
    .style("display", "block");

  var g = svg.append("g");

  /* --- Zoom --- */
  var zoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on("zoom", function(event) { g.attr("transform", event.transform); });
  svg.call(zoom);

  /* --- Defs: arrowheads, glow filter --- */
  var defs = svg.append("defs");

  defs.append("marker")
    .attr("id", "cog-arrow")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 28).attr("refY", 0)
    .attr("orient", "auto")
    .attr("markerWidth", 6).attr("markerHeight", 6)
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", T.arrow);

  defs.append("marker")
    .attr("id", "cog-arrow-dim")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 28).attr("refY", 0)
    .attr("orient", "auto")
    .attr("markerWidth", 6).attr("markerHeight", 6)
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", T.arrow)
    .attr("opacity", 0.15);

  /* Glow filter for selected node */
  var glow = defs.append("filter")
    .attr("id", "cog-glow")
    .attr("x", "-50%").attr("y", "-50%")
    .attr("width", "200%").attr("height", "200%");
  glow.append("feGaussianBlur").attr("stdDeviation", "4").attr("result", "blur");
  glow.append("feFlood").attr("flood-color", "#22c55e").attr("flood-opacity", "0.7").attr("result", "color");
  glow.append("feComposite").attr("in", "color").attr("in2", "blur").attr("operator", "in").attr("result", "glow");
  var glowMerge = glow.append("feMerge");
  glowMerge.append("feMergeNode").attr("in", "glow");
  glowMerge.append("feMergeNode").attr("in", "SourceGraphic");

  /* --- Force simulation --- */
  var simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(function(d) { return d.id; }).distance(140))
    .force("charge", d3.forceManyBody().strength(-400))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide().radius(50));

  /* --- Edges (quadratic Bezier) --- */
  var link = g.append("g")
    .selectAll("path")
    .data(links)
    .join("path")
    .attr("fill", "none")
    .attr("stroke", T.edge)
    .attr("stroke-opacity", 0.5)
    .attr("stroke-width", 1.8)
    .attr("marker-end", "url(#cog-arrow)");

  /* --- Edge labels --- */
  var linkLabels = g.append("g")
    .selectAll("text")
    .data(links)
    .join("text")
    .text(function(d) { return d.label; })
    .attr("text-anchor", "middle")
    .attr("fill", T.edgeLabel)
    .attr("font-size", "10px")
    .attr("font-weight", "400")
    .attr("pointer-events", "none")
    .attr("font-family", "system-ui, -apple-system, sans-serif")
    .style("text-shadow", T.textShadow);

  /* --- Hexagon helper --- */
  function hexPath(size) {
    var pts = [];
    for (var i = 0; i < 6; i++) {
      var a = i * Math.PI / 3;
      pts.push((i === 0 ? "M" : "L") + " " +
        (size * Math.cos(a)).toFixed(2) + "," +
        (size * Math.sin(a)).toFixed(2));
    }
    return pts.join(" ") + " Z";
  }

  /* --- Node groups --- */
  var node = g.append("g")
    .selectAll("g")
    .data(nodes)
    .join("g")
    .style("cursor", "pointer")
    .call(d3.drag()
      .on("start", function(event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
      })
      .on("drag", function(event, d) {
        d.fx = event.x; d.fy = event.y;
      })
      .on("end", function(event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null; d.fy = null;
      }));

  /* Hexagon shape */
  node.append("path")
    .attr("d", hexPath(20))
    .attr("fill", "#2F9CFF")
    .attr("class", "cog-hex");

  /* Node label (below hexagon) */
  node.append("text")
    .text(function(d) { return d.id; })
    .attr("text-anchor", "middle")
    .attr("dy", "35px")
    .attr("fill", T.nodeLabel)
    .attr("font-size", "11px")
    .attr("font-weight", "500")
    .attr("pointer-events", "none")
    .attr("font-family", "system-ui, -apple-system, sans-serif")
    .style("text-shadow", T.textShadow);

  /* --- Hover --- */
  node
    .on("mouseenter", function(event, d) {
      d3.select(this).select(".cog-hex")
        .transition().duration(150).attr("d", hexPath(24));
      var outC = links.filter(function(l) {
        var s = typeof l.source === "string" ? l.source : l.source.id;
        return s === d.id;
      }).length;
      var inC = links.filter(function(l) {
        var t = typeof l.target === "string" ? l.target : l.target.id;
        return t === d.id;
      }).length;
      tooltip
        .html("<strong>" + d.id + "</strong><br/><span style='color:" + T.tooltipDetail + "'>" +
              inC + " in &middot; " + outC + " out</span>")
        .style("left", (event.offsetX + 15) + "px")
        .style("top",  (event.offsetY - 10) + "px")
        .style("opacity", "1");
    })
    .on("mousemove", function(event) {
      tooltip
        .style("left", (event.offsetX + 15) + "px")
        .style("top",  (event.offsetY - 10) + "px");
    })
    .on("mouseleave", function() {
      if (!d3.select(this).classed("cog-selected")) {
        d3.select(this).select(".cog-hex")
          .transition().duration(150).attr("d", hexPath(20));
      }
      tooltip.style("opacity", "0");
    });

  /* --- Click-to-select --- */
  var selectedId = null;

  function getConnected(nodeId) {
    var cNodes = new Set([nodeId]);
    var cLinks = new Set();
    links.forEach(function(l, i) {
      var s = typeof l.source === "string" ? l.source : l.source.id;
      var t = typeof l.target === "string" ? l.target : l.target.id;
      if (s === nodeId || t === nodeId) {
        cNodes.add(s); cNodes.add(t); cLinks.add(i);
      }
    });
    return {nodes: cNodes, links: cLinks};
  }

  function applySelection(id) {
    selectedId = id;
    if (!id) {
      node.classed("cog-selected", false);
      node.select(".cog-hex").transition().duration(200)
        .attr("d", hexPath(20)).attr("fill", "#2F9CFF").attr("filter", null);
      node.transition().duration(200).style("opacity", 1);
      node.selectAll("text").transition().duration(200).style("opacity", 1);
      link.transition().duration(200).attr("stroke-opacity", 0.5).attr("marker-end", "url(#cog-arrow)");
      linkLabels.transition().duration(200).style("opacity", 1);
    } else {
      var conn = getConnected(id);
      node.classed("cog-selected", function(d) { return d.id === id; });
      node.select(".cog-hex").transition().duration(200)
        .attr("d", function(d) { return d.id === id ? hexPath(24) : hexPath(20); })
        .attr("fill", function(d) { return d.id === id ? "#22c55e" : "#2F9CFF"; })
        .attr("filter", function(d) { return d.id === id ? "url(#cog-glow)" : null; });
      node.transition().duration(200)
        .style("opacity", function(d) { return conn.nodes.has(d.id) ? 1 : 0.15; });
      node.selectAll("text").transition().duration(200)
        .style("opacity", function(d) { return conn.nodes.has(d.id) ? 1 : 0.15; });
      link.transition().duration(200)
        .attr("stroke-opacity", function(_, i) { return conn.links.has(i) ? 0.8 : 0.08; })
        .attr("marker-end", function(_, i) { return conn.links.has(i) ? "url(#cog-arrow)" : "url(#cog-arrow-dim)"; });
      linkLabels.transition().duration(200)
        .style("opacity", function(_, i) { return conn.links.has(i) ? 1 : 0.08; });
    }
  }

  node.on("click", function(event, d) {
    event.stopPropagation();
    applySelection(selectedId === d.id ? null : d.id);
  });

  svg.on("click", function() { applySelection(null); });

  /* --- Tick --- */
  simulation.on("tick", function() {
    link.attr("d", function(d) {
      var sx = d.source.x || 0, sy = d.source.y || 0;
      var tx = d.target.x || 0, ty = d.target.y || 0;
      var mx = (sx + tx) / 2, my = (sy + ty) / 2;
      var dx = tx - sx, dy = ty - sy;
      var len = Math.sqrt(dx * dx + dy * dy) || 1;
      var off = Math.min(30, len * 0.15);
      var cx = mx - (dy / len) * off;
      var cy = my + (dx / len) * off;
      return "M " + sx + "," + sy + " Q " + cx + "," + cy + " " + tx + "," + ty;
    });

    linkLabels
      .attr("x", function(d) { return ((d.source.x || 0) + (d.target.x || 0)) / 2; })
      .attr("y", function(d) {
        var sx = d.source.x || 0, sy = d.source.y || 0;
        var tx = d.target.x || 0, ty = d.target.y || 0;
        var dx = tx - sx, dy = ty - sy;
        var len = Math.sqrt(dx * dx + dy * dy) || 1;
        var off = Math.min(30, len * 0.15);
        return (sy + ty) / 2 + (dx / len) * off - 8;
      });

    node.attr("transform", function(d) { return "translate(" + (d.x || 0) + "," + (d.y || 0) + ")"; });
  });
})();
"""


def build_graph_html(graph_data, dark=False):
    """
    Build a self-contained HTML document that renders *graph_data* as an
    interactive D3.js force-directed graph.

    :param graph_data: dict with ``nodes`` and ``links`` lists (output of
        ``Graph.graph()``).
    :param dark: If True, use dark background theme.
    :return: HTML string.
    """
    js_src = D3_CDN
    bg = "#0f172a" if dark else "#ffffff"

    safe_json = json.dumps(graph_data).replace('<', '\\u003c').replace('>', '\\u003e')

    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>CogDB Graph</title>
  <style>
    body {{
      margin: 0; padding: 0;
      font-family: system-ui, -apple-system, sans-serif;
    }}
    #cog-graph-view {{
      position: relative;
      width: 100%; height: 100vh;
      background: {bg};
      overflow: hidden;
    }}
  </style>
  <script src="{js_src}"></script>
</head>
<body>
  <div id="cog-graph-view"></div>
  <script>
    var cogContainer = document.getElementById("cog-graph-view");
    var graphData = {graph_json};
    {theme_js}
    {viz_js}
  </script>
</body>
</html>""".format(js_src=js_src, graph_json=safe_json, bg=bg,
                  theme_js=_theme_js(dark), viz_js=_d3_graph_js())


def _iframe_srcdoc(graph_data, container_id, width, height, dark=False):
    """
    Build a minimal self-contained HTML page suitable for use as an
    ``<iframe srcdoc="...">`` value.  The page loads D3 via a normal
    ``<script src>`` tag (which works inside an iframe but *not* inside
    Jupyter's ``display(HTML(...))``).
    """
    bg = "#0f172a" if dark else "#ffffff"
    safe_json = json.dumps(graph_data or {"nodes": [], "links": []}) \
        .replace('<', '\\u003c').replace('>', '\\u003e')

    return """<!DOCTYPE html>
<html><head><meta charset="utf-8"/>
<style>
  body {{ margin:0; padding:0; font-family: system-ui, -apple-system, sans-serif; }}
  #{cid} {{ position:relative; width:{w}px; height:{h}px;
            background:{bg}; overflow:hidden; }}
</style>
<script src="{cdn}"></script>
</head><body>
<div id="{cid}"></div>
<script>
  var cogContainer = document.getElementById("{cid}");
  var graphData = {data};
  {theme}
  {viz}
</script>
</body></html>""".format(
        cid=container_id, w=width, h=height, bg=bg,
        cdn=D3_CDN, data=safe_json,
        theme=_theme_js(dark), viz=_d3_graph_js()
    )


class View(object):

    def __init__(self, url, html, graph_data=None):
        self.url = url
        self.html = html
        self.graph_data = graph_data

    def render(self, height=500, width=700, dark=False):
        """
        Render the graph view.

        Automatically detects the environment:
        - Google Colab / Jupyter notebook – renders the graph in the notebook.
        - Terminal / script – opens the graph in the default web browser. Falls back to printing the file path on headless servers.

        :param height: Height of the rendered view in pixels.
        :param width: Width of the rendered view in pixels.
        :param dark: If True, use dark background theme.
        :return: None
        """
        container_id = "cog-graph-" + uuid.uuid4().hex[:8]
        bg = "#0f172a" if dark else "#ffffff"

        # --- Detect environment ---
        _in_colab = False
        _in_notebook = False

        try:
            import google.colab  # noqa: F401
            _in_colab = True
        except ImportError:
            pass

        if not _in_colab:
            try:
                shell = get_ipython().__class__.__name__  # noqa: F821
                _in_notebook = (shell == "ZMQInteractiveShell")
            except NameError:
                pass  # not in IPython at all

        # --- Colab path ---
        if _in_colab:
            from IPython.core.display import display, HTML
            from google.colab import output as colab_output

            graph_json = json.dumps(self.graph_data or {"nodes": [], "links": []})
            safe_json = graph_json.replace('<', '\\u003c').replace('>', '\\u003e')

            display(HTML(
                '<div id="{cid}" '
                'style="position:relative;width:{w}px;height:{h}px;'
                'background:{bg};border-radius:8px;overflow:hidden;">'
                '</div>'.format(cid=container_id, w=width, h=height, bg=bg)
            ))

            colab_js = """
            (function() {{
                var script = document.createElement("script");
                script.src = "{cdn}";
                document.head.appendChild(script);
                script.onload = function() {{
                    var cogContainer = document.getElementById("{cid}");
                    var graphData = {data};
                    {theme}
                    {viz}
                }};
            }})();
            """.format(cdn=D3_CDN, cid=container_id,
                       data=safe_json,
                       theme=_theme_js(dark), viz=_d3_graph_js())

            colab_output.eval_js(colab_js)

        # --- Jupyter notebook path ---
        elif _in_notebook:
            import warnings
            from IPython.core.display import display, HTML

            html_content = _iframe_srcdoc(self.graph_data, container_id,
                                          width, height, dark=dark)
            # Escape for embedding inside an HTML attribute
            escaped = html_content.replace('&', '&amp;').replace('"', '&quot;')

            iframe_tag = (
                '<iframe srcdoc="{doc}" '
                'width="{w}" height="{h}" '
                'style="border:none;border-radius:8px;"'
                '></iframe>'
            ).format(doc=escaped, w=width, h=height)

            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", message="Consider using IPython.display.IFrame"
                )
                display(HTML(iframe_tag))

        # --- Terminal / script path ---
        else:
            import tempfile
            import webbrowser

            html_content = build_graph_html(
                self.graph_data or {"nodes": [], "links": []}, dark=dark
            )
            tmp = tempfile.NamedTemporaryFile(
                suffix=".html", delete=False, mode="w", encoding="utf-8"
            )
            tmp.write(html_content)
            tmp.close()

            try:
                webbrowser.open("file://" + tmp.name)
            except webbrowser.Error:
                print("Graph saved to: " + tmp.name)

    def show(self, height=500, width=700, dark=False):
        """
        Convenience alias for :meth:`render`.

        Renders the graph view inline in a Jupyter or Google Colab
        notebook. Equivalent to calling ``view.render(height, width)``.

        :param height: Height of the rendered view in pixels.
        :param width: Width of the rendered view in pixels.
        :param dark: If True, use dark background theme.
        :return: None
        """
        return self.render(height=height, width=width, dark=dark)

    def persist(self, path=None):
        """
        Save the view HTML to a file.

        :param path: Optional override path. Defaults to ``self.url``.
        """
        target = path or self.url
        if target:
            with open(target, "w") as f:
                f.write(self.html)

    @staticmethod
    def extract_graph_data(html):
        """
        Extract graph data JSON from a persisted view HTML file.

        :param html: HTML string from a persisted view.
        :return: dict with ``nodes`` and ``links``, or None if not found.
        """
        m = re.search(r'var graphData\s*=\s*(\{.*?\});', html, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except (json.JSONDecodeError, ValueError):
                return None
        return None

    def __str__(self):
        return self.url