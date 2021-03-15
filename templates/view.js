document.addEventListener('DOMContentLoaded', function() {

    results = {{RESULT_PLACEHOLDER}}

    var g = {
        nodes: [],
        edges: []
    }

    function isEqual(a, b) {
        return JSON.stringify(a) === JSON.stringify(b);
    }

    function getNode(resultNode) {
        return {
            'data': {
                id: resultNode,
                label: resultNode
            },
            classes: 'nodeIcon'
        }
    }

    for (var i = 0; i < results.length; i++) {
        result = results[i];
        g.nodes.push(getNode(result["source"]));
        g.nodes.push(getNode(result["target"]));

        g.edges.push({
            'data': {
                source: result["source"],
                target: result["target"]
            }
        })

    }

    var cy = window.cy = cytoscape({
        container: document.getElementById('graph-container'),
        autounselectify: true,
        boxSelectionEnabled: false,
        layout: {
            name: 'cola'
        },

        style: [{
                selector: 'node',
                css: {
                    'label': 'data(id)',
                    'text-valign': 'bottom',
                    'text-halign': 'center',
                }
            },
            {
                selector: 'edge',
                css: {}
            }
        ],

        elements: g
    });
});