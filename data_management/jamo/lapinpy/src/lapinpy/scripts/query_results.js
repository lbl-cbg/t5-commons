$('.results_table').one(function($table){
	$table.bind('scroll',function () {
		var $ths = $table.find('thead th'),
			index = 0,
			count = $ths.length,
			top = $table.position().top - $table.find('table').position().top;

		if (top < 0) {
			top = 0;
		}

		for (; index < count; index++){
			$ths[index].style.top = top;
		}
})});

function query_results_initialize($scope,$http,$location,$modal) {
	var functions_loaded = 0,
		paging = functions.indexOf('pager') != -1,
		multiSelect = functions.indexOf('multi_select') != -1,
		head = document.getElementsByTagName('head')[0],
		index = 0,
		count = functions.length;

	$scope.table = $('table.qresults');
	$scope.no_results = $scope.table.find('tr.noResults');
	$scope.headers = data.headers;
	$scope.result_info = $('.result_info');
	$scope.multiSelect = data.multi_select;
	$scope.columns = data.columns;
	$scope.data = data.data;
	$scope.id_field = data.id_field;
	$scope.record_per_page = $scope.data.record_per_page;
	$scope.result_desc = data.result_desc;
	$scope.page_results = {};
	$scope.page_results_cached = 0;
	$scope.sort = data.sort;
	$scope.sort_change = false;
	$scope.download = $scope.data.url;
	if ($scope.download != undefined && ($scope.download.startsWith('/api/') || $scope.download.startsWith('api/'))){
		$scope.download = $scope.download.substring($scope.download.indexOf('api/')+4);
	}

	$scope.stored_amount=0;
	$scope.error_code = 0;
	$scope.error_message = '';
	$scope.fields = data.fields;
	$scope.results_info = $scope.table.parent().siblings('.result_info');
	$scope.total_display = $scope.results_info.find('.result_desc > span');

	if ($scope.data['default_query'] != null) {
		var result = {'results':data.tbody, 'result_desc':$scope.result_desc};
		$scope.query = $scope.data['default_query'];
		$scope.current_results = result;
		$scope.page_results[1] = result;
	} else {
		$scope.query = undefined;
	}

	$scope.default_query = undefined;
	$scope.last_query = $scope.query;

	$scope.savedqueries = functions.indexOf('filter_savedqueries') != -1;

		$queryresults.one('functions_loaded', function(event){
		if($scope.query && typeof $scope.useSavedQuery !== 'undefined'){
			$scope.useSavedQuery($scope.query);
		}

		if (paging && $scope.result_desc.total > 0){
			updatePaging($scope);
		}

		$('.filterkey').trigger('change');
	}).off('click', 'table tbody td .button').on('click', 'table tbody td .button', function(){
		var $this = $(this),
			method = $this.attr('method'),
			url = $this.attr('url');

		if (method == 'GET') {
			$http.get(url).success(function(){
				$scope.button_success($this);
			}).error(function(data){
				$scope.error(data);
			});
		} else {
			$http.post(url, $this.data()).success(function(){
				$scope.button_success($this);
			}).error(function(data){
				$scope.error(data);
			});
		}
	});

	for(;index < count; index++){
		var func = functions[index];
		window['initialize_' + func]($scope,$http,$location,$modal);
		functions_loaded++;
		if (functions_loaded == count){
			$queryresults.trigger('functions_loaded');
		}
	}

	if (data['pageScript'] != undefined && data['pageScript'] != '') {
		$http.get('/api/core/jsscript/' + encodeURI(data['pageScript'])).success(function(result){
			$queryresults.append('<script type="text/javascript">' + result + '<\/script>');
		});
	}

	$scope.error = function(data){
		$scope.error_code = data.error_code;
		if (data.errors == undefined){
			$scope.error_message = data;
		} else if (data.errors.length > 1) {
			$scope.error_message = data.errors[0];
		} else {
			$scope.error_message = data.errors;
		}
		$modal({scope:$scope, template:"/api/core/htmltemplate/error_message.tpl.html"});
	};

	$scope.button_success = function($this){
		var name = $this.val().trim().replace(' ', '_').toLowerCase(),
			$row = $this.closest('tr'),
			event = $.Event(name + '_click', {'row':$row});

		if ($this.hasClass('bool')) {
			toggleBool($this);
		}

		$('.queryresults').trigger(event);
	};

	$scope.table.off('click', 'thead th.sort').on('click', 'thead th.sort', function(){
		var $this = $(this),
			name = $this.attr('name'),
			$span = $this.children('.sort_indicator');

		if ($scope.sort != '' && !$scope.sort.startsWith(name + ' ')){
			$scope.table.find('th.sort > span.arrow').removeClass('arrow up down');
		}

		if ($span.hasClass('up')){
			$span.removeClass('up').addClass('down');
			$scope.sort = name  + ' desc';
		} else if ($span.hasClass('down')) {
			$span.removeClass('down arrow');
			$scope.sort = '';
		} else {
			$span.addClass('up arrow');
			$scope.sort = name + ' asc';
		}
		$scope.sort_change = true;
		$scope.dataChange(1, true, $this);
	});

	$scope.changeDataSuccess = function(page, newSearch, $refElement){
		// TODO: This is the first step to multiple tables on a page that do stuff
		//		Paging should be updated
		if ($refElement == undefined) {
			$scope.table.find('tbody > tr').remove(':not(:first)').after($scope.current_results.results);
		} else {
			$refElement.closest('.results').find('tbody > tr').remove(':not(:first)').after($scope.current_results.results);
		}

		if (page != $scope.current_page){
			if (paging) {
				var formatted_page = formatInteger(page);
				$scope.page_input.attr('value', formatted_page).val(formatted_page);
			}
			$scope.current_page = page;
		}

			$scope.last_query = $scope.query;
			if (paging) {
				updatePaging($scope);
			}

			if (multiSelect){
				$scope.processMultiSelectOnPageChange(newSearch);
			}
	};
	$scope.disableDataChange = function(){
		$scope.filter_enabled = false;
		if (paging){
			$scope.page_arrows.addClass('disabled');
			$scope.page_input.attr('disabled','disabled');
		}
	};
	$scope.updateCachedPages = function(page){
		var pageRows = $($scope.current_results.results).length;

		//Clear stored results after 10,000
		if ($scope.page_results_cached + pageRows > 10000) {
			$scope.page_results = {};
			$scope.page_results_cached = 0;
		}

		$scope.page_results[page] = $scope.current_results;
		$scope.page_results_cached += pageRows;
	};
	$scope.dataChange = function(page, newSearch, $refElement){
		if (newSearch && $scope.last_query == $scope.query && !$scope.sort_change){
			return;
		}
		var results = (newSearch)? undefined: $scope.page_results[page],
			current_results = null,
			allSelected = true,
			index = 0,
			count = 0;

		$scope.disableDataChange();
		if (results == undefined){
			var query = $scope.query;

			if ($scope.default_query != undefined && $scope.query != $scope.default_query){
				query += ' ' + $scope.default_query;
			}

			$http.post('/api/core/queryResults_dataChange',{'query':query.trim(),
															'fields':$scope.fields,
															'return_count':$scope.record_per_page,
															'columns':$scope.columns,
															'multi_select':$scope.multiSelect,
															'page': page,
															'id_field':$scope.id_field,
															'sort':$scope.sort,
															'db_address': $scope.data.db_address}
			).success(function(data) {
				var trs = data.tbody;
				if (newSearch){
					$scope.page_results = {};
					if (multiSelect){
						$scope.deselect_all();
					}
				}

				if ($scope.result_desc.total != data.total){
					$scope.total_display.html('Total: ' + data.total_formatted);

					if (data.total > 0){
						$scope.no_results.addClass('hideElement');
					} else {
						$scope.no_results.removeClass('hideElement');
					}
				}

				$scope.result_desc = data;
				$scope.current_results = {'results':trs, 'result_desc':$scope.result_desc};
				$scope.updateCachedPages(page);
				$scope.changeDataSuccess(page, newSearch, $refElement);
				$scope.filter_enabled = true;
		}).error(function(data) {
			$scope.error(data);
			updatePaging($scope);
			$scope.filter_enabled = true;
		});
		} else {
			$scope.current_results = results;
			$scope.result_desc = results.result_desc;
			$scope.changeDataSuccess(page, newSearch, $refElement);
		}
		if ($scope.sort_change) {
			$scope.sort_change = false;
		}
	};
}

function toggleBool($bool){
	var value = $bool.attr('value'),
		new_val = "True",
		text = "\u2713";

	if (value == "True"){
		new_val = "False";
		text = 'x'
	}

	$bool.attr("value",new_val).attr("title", new_val).text(text);
}