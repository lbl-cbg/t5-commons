function initialize_multi_select($scope,$http,$location,$modal) {
	$scope.selected_results = [];
	$scope.action_buttons = $scope.table.closest('.search_window').children('.select_actions').find('button');
	$scope.selectedResults = null;
	$scope.selection_process = null;
	$scope.selectAll = $('.results_table thead .q_select input');
	$scope.select = {'options': $scope.multiSelect.services,
		'actions_enabled':false,
		'count':0,
		'info':0};
	$scope.additional_info_ms = $scope.multiSelect.additional_info;
	$scope.additional_info_ms_parts = [];
	$scope.additional_info_ms_display_parts = [];
	if ($scope.additional_info_ms != undefined && $scope.additional_info_ms != '') {
		$scope.additional_info_const = false;
		$scope.additional_info_conversion = false;
		if ($scope.additional_info_ms.indexOf('[[') != -1) {
			if ($scope.additional_info_ms.indexOf(' + ') != -1 ||
					$scope.additional_info_ms.indexOf(' - ') != -1 ||
					$scope.additional_info_ms.indexOf(' * ') != -1 ||
					$scope.additional_info_ms.indexOf(' / ') != -1)
				{
	    			$scope.additional_info_conversion = true;		    			
				}
			$scope.additional_info_ms_parts = $scope.additional_info_ms.split(' ');
		} else {
			$scope.additional_info_const = true;
			$scope.additional_info_ms_display_parts[0] = $scope.additional_info_ms;
		}
	}
	
	//region: Get modal templates
	$scope.view_selected = function(){
		$scope.selectedResults = $modal({scope:$scope, template:"/api/core/htmltemplate/selected_rows.tpl.html"});
	};
	$scope.confirm_select_action = function(opt){
		$scope.selection_process = opt;
		$scope.selectedResults = $modal({scope:$scope, template:"/api/core/htmltemplate/select_row_action_confirmation.tpl.html"});
	};
	$scope.process_selection = function(callback, id_return, user_return){
		var ids = $scope.get_selected_ids(),
			pass_back = {'id_return': ids};

		pass_back[id_return] = ids;
		if (user_return != undefined) {
			pass_back[user_return] = this.user;
		}

		$http.post(callback,pass_back).success(            
			function(data, status, headers, config) {
		    		$scope.selectedResults.hide();
	            $scope.deselect_all();
	       }).error(function(data) {
       			$scope.error_code = data.error_code;
       			$scope.error_message = data.errors[0];
       			$modal({scope:$scope, template:"/api/core/htmltemplate/error_message.tpl.html"});
	   	}); 
	};
	$scope.get_selected_ids = function(){
		var results = $scope.selected_results,
			ids = [],
			index = 0,
			count = results.length;
		
		for(; index < count; index++){
			ids.push(results[index].id);
		}
		
		return ids;
	};
	$scope.get_selected_from_results = function(selected, results){
		var index = 0,
			count = results.length,
			result = null;
		
		for (; index < count; index++){
			result = results[index];
			if (result.selected){
				selected.push(result);
			}
		}
		
		return selected;
	};
	$scope.deselect = function(id){
		if ($scope.table.find('tr[data-id="' + id + '"]').length > 0){
	    		$scope.selectAll.prop('checked', false);
		}
		
		$('.modal.selected').find('table tbody').find('tr').filter('[data-id='+id+']').remove();	
		$scope.remove_selected_results(id);
	};
	$scope.remove_selected_results = function(id){
		var results = $scope.selected_results,
			index = 0,
			count = results.length,
			removeAtIndex = false,
			$row = null;
			
		for (; index < count; index++) {
			if (results[index].id == id) {
				removeAtIndex = true;
				break;
			}
		}
		
		if (removeAtIndex) {
			$scope.table.find('tbody tr').filter(function(){
				if ($(this).attr('data-id') == id){
					$row = $(this);
				}
			});
			if ($row != null) {
				$row.children('.q_select').find('input').click();
			} else {
				$scope.selected_results.splice(index,1);
				$scope.update_selected_count(1, false);
			}
		}
	};
	$scope.deselect_all = function(){
		var selected_count = $scope.selected_results.length;
		if (selected_count > 0){	
		    	if (!$scope.additional_info_const) {
		    		$scope.additional_info_ms_display_parts = [];
		    	}
		    	$scope.selected_results = []; 
		    	$scope.update_selected_count(selected_count, false);
		    	$scope.selectAll.prop('checked', false);
		    	$scope.table.find('tr td.q_select input:checked').removeAttr('checked');
		}
	};
	//endregion: Get modal tempalges
	$scope.select_all_tr = function(){
		var checked = $scope.selectAll.is(':checked'),
			results = $scope.table.find('tbody tr').filter(':not(.hideElement)'),
			result = null,
			index = 0,
			count = results.length,
			selected = false,
			select_changed = 0,
			id = null;
			
			for (;index < count; index++) {
				result = results.eq(index);
				selected = result.find('.q_select input').is(':checked');
				if (!selected != !checked){
					select_changed += 1;
	    				$scope.set_additional_info_ms(result[0], checked);
				}
				
				if (checked){
					result.find('.q_select input').attr('checked', 'checked').prop('checked', true);
				} else {
					result.find('.q_select input').removeAttr('checked');
				}
				$scope.selectionChanged(result, checked);
			}    		
	
	    	$scope.update_selected_count(select_changed, checked);
	};
	$scope.selectionChanged = function(row, checked){
		if (checked){
			var $row = $(row),
				$tds = $row.children(),
				$td = null,
				index = 0,
				count = $tds.length,
				values = [];
			for(;index < count;index++){
				$td = $tds.eq(index);
				if (!$td.hasClass('q_select')){
					values.push($td.html());
				}
			}
			$scope.selected_results.push({'id':$row.attr('data-id'), 'cells':values});
		} else {
			var index = 0,
				count = $scope.selected_results.length,
				id = $(row).attr('data-id'),
				selected_id = null;
			for(;index < count; index++){
				if ($scope.selected_results[index].id == id){
					$scope.selected_results.splice(index,1);
					break;
				}
			}
		}
	};
	$scope.table.off('change', 'tbody .q_select input').on('change', 'tbody .q_select input', function(event){
		var $this = $(this),
			checked = $this.is(':checked'),
			index = $this.closest('tr').index() - 1,
			row = $($scope.current_results.results)[index];
		$scope.set_additional_info_ms(row, checked);
		$scope.update_selected_count(1, checked);
		$scope.selectionChanged(row, checked);
	});	
	$scope.set_additional_info_ms = function(result, add){
		if (!$scope.additional_info_const) {
			var parts = $scope.additional_info_ms_parts,
				part = '',
				resultField = false,
				current_parts = $scope.additional_info_ms_display_parts,
				current_part = '',
				index = 0,
				count = parts.length,
				value = '',
				tds = null;
			
			for (; index < count; index++) {
				part = parts[index];
				resultField = part.startsWith('[[');
				current_part = $scope.additional_info_ms_display_parts[index];
				if (current_part == undefined || current_part == null || current_part == '') {
					if (resultField) {
						current_part = '0';
					} else {
						$scope.additional_info_ms_display_parts[index] = part;
					}
				}
				if (resultField) {
					if (tds == null){
						tds = $(result).children(':not(.q_select, .hideElement)');
					}
					part = part.replace('[[', '').replace(']]', '');
					value = tds.eq($scope.fields.indexOf(part)).text();
					
					//Assuming simple conversions
					if ($scope.additional_info_conversion) {
						value = removeNumberFormat(value)
						switch(parts[index + 1]){
							case '+':
								value += Number(parts[index + 2]);
								break;
							case '-':
								value -= Number(parts[index + 2]);
								break;
							case '*':
								value = value * Number(parts[index + 2]);
								break;
							case '/':
								value = value / Number(parts[index + 2]);
								break;
						}		    			
					}		
					value = value.toFixed(3);
					
					if (add) {
						value = Number(current_part.replace(/,/g, '')) + Number(value);
					} else {
						value = Number(current_part.replace(/,/g, '')) - Number(value);
					}
					value = value.toFixed(3);
	
	    				$scope.additional_info_ms_display_parts[index] = formatFloat(value, 3);
					    			
					if ($scope.additional_info_conversion) {
						index += 2;
					}
				}	
			}
		}
	};	
	$scope.update_selected_count = function(value, add){
		var count = $scope.select.count,
			message = '';
		
		if (add) {
	    		count = count + value;
		} else {
			count = count - value;
		}
		
		$scope.select.count = count;
	    
	    if (count == 0){
	    		$scope.select.info = 0;
	    	
		    	if ($scope.selectedResults != null) {
		    		$scope.selectedResults.hide();
		    	}
	    } else {
		    	message = formatInteger(count);
		    	if ($scope.additional_info_ms_display_parts.length > 0) {
		    		message += ' (' + $scope.additional_info_ms_display_parts.join(' ') + ')'
		    	}
	    		$scope.select.info = message;
	    }
	    $scope.result_info.find('.selected_count').html($scope.select.info);
	    if (count > 0) {
	    		$scope.action_buttons.removeAttr('disabled');
	    } else {
	    		$scope.action_buttons.attr('disabled', 'disabled');
	    }
	};
	$scope.processMultiSelectOnPageChange = function(newSearch){
		if ($scope.selected_results.length > 0){
			if (newSearch){
				$scope.deselect_all();
			} else {
		    		var $rows = $scope.table.find('tbody tr:not(.hideElement)'),
		    			$row = null,
		    			index = 0,
		    			count = $rows.length,
		    			ids = $scope.get_selected_ids(),
		    			selected_count = 0;
		    		
		    		for (;index < count; index++){
		    			$row = $rows.eq(index);
		    			if (ids.indexOf($row.attr('data-id')) != -1) {
		    				selected_count++;
		    				$row.find('.q_select input').attr('checked', 'checked').prop('checked', true);
		    			}
		    		}
		    		
		    		$scope.selectAll.prop('checked', selected_count == count);
			}
		}
	};
}