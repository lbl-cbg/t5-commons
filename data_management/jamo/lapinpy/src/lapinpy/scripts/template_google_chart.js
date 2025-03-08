google.load('visualization', '1.0', {packages: ['charteditor']});
var chartEditor = null,
    chart = null,
    wrapper = null;

function drawVisualization() {
     document.getElementById('table').style.height='400px';
     if (perms != undefined && perms.includes('admin')){
        var buttonnode = document.createElement('input');
        buttonnode.setAttribute('type', 'button');
        buttonnode.setAttribute('name', 'sal');
        buttonnode.setAttribute('value', 'Edit Chart');
        document.getElementById('edit_chart').appendChild(buttonnode);
        buttonnode.onclick = editChart
    }

   wrapper = new google.visualization.ChartWrapper(chartDetails);
     chartEditor = new google.visualization.ChartEditor();
     google.visualization.events.addListener(chartEditor, 'ok', redrawChart);
     wrapper.draw(document.getElementById('table'))
}

function editChart(){
     chartEditor.openDialog(wrapper, {});
}

function redrawChart(){
     var xhr = new XMLHttpRequest();

     wrapper = chartEditor.getChartWrapper();
     chartEditor.getChartWrapper().draw(document.getElementById('table'));
     xhr.open("POST","/api/core/chart",true);
     xhr.setRequestHeader("Content-type",'application/x-www-form-urlencoded');
     xhr.send("wrapper="+JSON.stringify(wrapper).replace(/\\\"/g,"'").replace(/\"/g,"")+"&chart=" + dataSourceUrl);
}
google.setOnLoadCallback(drawVisualization);