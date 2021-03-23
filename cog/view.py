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

    results ={plot_data_insert} """

script_part2 = r"""

    var nodes = new vis.DataSet();
    var edges = new vis.DataSet();
    for (let i = 0; i < results.length; i++) {
        res = results[i];
        nodes.update({
            id: res.from,
            label: res.from
        });
        nodes.update({
            id: res.to,
            label: res.to
        });
        edges.update({
            from: res.from,
            to: res.to
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
            font: "12px arial #ff0000",
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