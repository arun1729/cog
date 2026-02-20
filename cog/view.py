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