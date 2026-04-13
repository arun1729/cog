script_part1 =r"""
<!DOCTYPE html>
<html lang="en">
  <head>
    <title>Cog Graph</title>
    <style type="text/css">
       body {
        padding: 0;
        margin: 0;
        width: 100%;!important; 
        height: 100%;!important; 
      }

      #cog-graph-view {
        width: 700px;
        height: 700px;
      }
    </style>
"""

graph_lib_src = r"""

    <script
      type="text/javascript"
      src="{js_src}"
    ></script>
  </head>  
"""

graph_template = r""" 
  <body>
    <div id="cog-graph-view"></div>

    <script type="text/javascript">

    var graphData ={plot_data_insert} """

script_part2 = r"""

    var nodes = new vis.DataSet();
    var edges = new vis.DataSet();

    for (var i = 0; i < graphData.nodes.length; i++) {
        var n = graphData.nodes[i];
        nodes.update({ id: n.id, label: n.id });
    }

    for (var i = 0; i < graphData.links.length; i++) {
        var link = graphData.links[i];
        edges.update({
            id: link.source + "-" + link.label + "-" + link.target,
            from: link.source,
            to: link.target,
            label: link.label || ""
        });
    }

    var container = document.getElementById("cog-graph-view");
    var data = {
        nodes: nodes,
        edges: edges,
    };
    var options = {
        nodes: {
            font: {
                size: 20,
                color: "black"
            },
            color: "#46944f",
            shape: "dot",
            widthConstraint: 200,

        },
        edges: {
            font: { size: 12, color: "#555555", strokeWidth: 0, align: "middle" },
            scaling: {
                label: true,
            },
            shadow: true,
            smooth: true,
            arrows: { to: {enabled: true}}
        },
        physics: {
            barnesHut: {
                gravitationalConstant: -30000
            },
            stabilization: {
                iterations: 1000
            },
        }

    };
    var network = new vis.Network(container, data, options);
    </script>
  </body>
</html>

"""


class View(object):

    def __init__(self, url, html):
        self.url = url
        self.html = html

    def render(self, height=700, width=700):
        """
        :param self:
        :param height:
        :param width:
        :return:
        """
        iframe_html = r"""  <iframe srcdoc='{0}' width="{1}" height="{2}"> </iframe> """.format(self.html, width,
                                                                                                height)
        from IPython.core.display import display, HTML
        display(HTML(iframe_html))

    def persist(self):
        f = open(self.url, "w")
        f.write(self.html)
        f.close()

    def __str__(self):
        return self.url