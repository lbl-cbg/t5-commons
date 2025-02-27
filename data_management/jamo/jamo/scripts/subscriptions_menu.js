$('#queryresults').off('toggle_click').on('toggle_click',function(data){
	toggleBool(data.row.find(".bool"));
});