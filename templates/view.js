document.addEventListener('DOMContentLoaded', function(){

results =  [{"id": "Harry Potter and the Half-Blood Prince (Harry Potter, #6)", "source": "J.K. Rowling, Mary GrandPr\\u00e9", "target": "Harry Potter and the Half-Blood Prince (Harry Potter, #6)"}, {"id": "Harry Potter and the Deathly Hallows (Harry Potter, #7)", "source": "J.K. Rowling, Mary GrandPr\\u00e9", "target": "Harry Potter and the Deathly Hallows (Harry Potter, #7)"}, {"id": "Harry Potter and the Goblet of Fire (Harry Potter, #4)", "source": "J.K. Rowling, Mary GrandPr\\u00e9", "target": "Harry Potter and the Goblet of Fire (Harry Potter, #4)"}, {"id": "Harry Potter and the Chamber of Secrets (Harry Potter, #2)", "source": "J.K. Rowling, Mary GrandPr\\u00e9", "target": "Harry Potter and the Chamber of Secrets (Harry Potter, #2)"}, {"id": "Harry Potter and the Order of the Phoenix (Harry Potter, #5)", "source": "J.K. Rowling, Mary GrandPr\\u00e9", "target": "Harry Potter and the Order of the Phoenix (Harry Potter, #5)"}, {"id": "Harry Potter and the Sorcerer\'s Stone (Harry Potter, #1)", "source": "J.K. Rowling, Mary GrandPr\\u00e9", "target": "Harry Potter and the Sorcerer\'s Stone (Harry Potter, #1)"}, {"id": "4.54", "source": "Harry Potter and the Half-Blood Prince (Harry Potter, #6)", "target": "4.54"}, {"id": "4.61", "source": "Harry Potter and the Deathly Hallows (Harry Potter, #7)", "target": "4.61"}, {"id": "4.53", "source": "Harry Potter and the Goblet of Fire (Harry Potter, #4)", "target": "4.53"}, {"id": "4.37", "source": "Harry Potter and the Chamber of Secrets (Harry Potter, #2)", "target": "4.37"}, {"id": "4.46", "source": "Harry Potter and the Order of the Phoenix (Harry Potter, #5)", "target": "4.46"}, {"id": "4.44", "source": "Harry Potter and the Sorcerer\'s Stone (Harry Potter, #1)", "target": "4.44"}]

var g = {
        nodes: [],
        edges: []
        }


    function isEqual(a, b) {
      return JSON.stringify(a) === JSON.stringify(b);
    }

    function getNode(resultNode){
       return { 'data' : {
            id : resultNode,
            label : resultNode
            },
            classes: 'nodeIcon'
        }
    }


    for (var i = 0; i < results.length; i++) {
        result = results[i];
        g.nodes.push(getNode(result["source"]));
        g.nodes.push(getNode(result["target"]));

        g.edges.push({'data' : {
              source: result["source"],
              target: result["target"]
                }
        })

      }
    console.log(g);
    var cy = window.cy = cytoscape({
        container: document.getElementById('gcontainer'),
        autounselectify: true,
        boxSelectionEnabled: false,
        layout: {
            name: 'cola'
        },

        style: [ {
                selector: 'node',
                css: {
                         'label': 'data(id)',
                        'text-valign': 'bottom',
                        'text-halign': 'center',
                }
            },
            {
                selector: 'edge',
                css: {
                }
            }
        ],

        elements: g
        });
});