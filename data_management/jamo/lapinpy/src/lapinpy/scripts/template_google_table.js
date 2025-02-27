google.load('visualization', '1', {'packages' : ['table']});

if (typeof init != "undefined"){
        google.setOnLoadCallback(init);
}

var query = null,
    options = null,
    container = null;

function init() {
    var parms = window.location.search.substring(1);
    if(parms=="")
        query = new google.visualization.Query(dataSourceUrl);
    else{
        var vars = parms.split("&"),
            query_string = {},
            i=0;
        for (i;i<vars.length;i++) {
          var pair = vars[i].split("=");
          // If first entry with this name
          if (typeof query_string[pair[0]] === "undefined") {
            query_string[pair[0]] = pair[1];
              // If second entry with this name
          } else if (typeof query_string[pair[0]] === "string") {
            var arr = [ query_string[pair[0]], pair[1] ];
            query_string[pair[0]] = arr;
            // If third or later entry with this name
          } else {
            query_string[pair[0]].push(pair[1]);
          }
        }
        query=new google.visualization.Query(dataSourceUrl,{'send_method':'makeRequest','makeRequestParams':query_string});
    }
    container = document.getElementById("table");
    options = {'pageSize': 30,'allowHtml':true,'showRowNumber':false};
    sendAndDraw();
}

function sendAndDraw() {
   query.abort();
   var tableQueryWrapper = new TableQueryWrapper(query, container, options);
   tableQueryWrapper.sendAndDraw();
}

function setOption(prop, value) {
   options[prop] = value;
   sendAndDraw();
}