google.load('visualization', '1', {packages:['table']});
var sizeArray = ['B','K','M','G','T','P'];
var searchTable;
var resultTable;
var resultData;
var searchResults = {};
var results = {};
//var scripts = ['https://www.google.com/jsapi','http://127.0.0.1:8090/scripts/common.js'];
var scripts = ['http://127.0.0.1:8090/scripts/common.js'];

function load_selector(element){
    var bodyEl = document.body;
    for( i in scripts){
        var scriptEl = document.createElement('script');
        scriptEl.type = 'text/javascript';
        scriptEl.src = scripts[i];
        bodyEl.appendChild(scriptEl);
    }
    container = document.getElementById(element)
    container.innerHTML='<div id="topform"><select id="search_field" class="sdm-select"><option value="metadata.library_name">library name</option></select><input name="value" id="search_value" class="s"></input><button style="margin-right:50px" name="search" class="button" onclick="fetchfiles();">Search</button></div><div id="search_table" class="file_table"></div>';
    
}

function convert(size){
    var on = 1;
    for(s in sizeArray){
        compare = on*1024;
        if(compare>size){
            return ''+(size/on).toFixed(2)+sizeArray[s];
        }
        on = compare
    }
    return size+'b';
}

function addFile(id){
    if(id in results)
        return;
    file = searchResults[id];
    results[id]=file
    resultData.addRow([file['file_name'],file['user'],{v:file['file_size'],f:convert(file['file_size'])},'<button class="button" onclick="removeFile('+file['_id']+');">remove</button>']);
    resultTable.draw(resultData, {showRowNumber: true, allowHtml:true});

}

function displayFiles(data){
    var gData = new google.visualization.DataTable();
    gData.addColumn('string','File Name');
    gData.addColumn('string','User');
    gData.addColumn('number','File Size');
    gData.addColumn('string','Select');
        for(file in data){
            file=data[file];
            searchResults[file['_id']]=file
            gData.addRow(['<a href="/metadata/file/'+file['_id']+'" target="_blank" >'+file['file_name']+'</a>',file['user'],{v:file['file_size'],f:convert(file['file_size'])},'<button class="button" onclick="addFile(\''+file['_id']+'\');">select</button>']);
        }
    searchTable = new google.visualization.Table(document.getElementById('search_table'));
    searchTable.draw(gData, {showRowNumber: true, allowHtml:true});
}
function fetchfiles(){
    var fields = document.getElementById('search_field');
    var field = fields.options[fields.selectedIndex].value;
    var value = document.getElementById('search_value').value;
    var reqData = {'return':['file_name','user','file_size','_id'],'query':{'file_type':'fastq'}};
    reqData['query'][field]=value;
    sendRequest('https://sdm-dev.jgi-psf.org:8034/api/metadata/search',displayFiles,reqData);

}
