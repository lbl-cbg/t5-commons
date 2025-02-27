function initialize_filter($scope,$http,$location,$modal) {
	var index = 0,
		count = 0,
		values = null;

	$scope.filter = data['filter'];
	$scope.filter.filterkey = {};
	for (var key in $scope.filter.search_keys){
		values = $scope.filter.search_keys[key];
		values['key'] = key;
	    $scope.filter.filterkey[key]=values;
	}
	$scope.search = $scope.table.closest('.search_window');
	$scope.filterop = $scope.search.find('.filterop');
	
	$scope.current_filter = {'operator':'='};
	$scope.allow_empty_query = $scope.filter.allow_empty_query;
	$scope.filter_enabled = false;
	$scope.conditions = [];
	$scope.filter_value = $('.search_filter .add_query_value .filterval');
	
	if ($scope.filter['always_use_default_query'] != null && $scope.filter['always_use_default_query']){
		var not_where_clause_indicators = ['left', 'right', 'join', 'inner', 'outer'],
			default_start = $scope.query.trim().split(' ')[0].toLowerCase();
		
		if (!not_where_clause_indicators.includes(default_start)){
			$scope.default_query = 'and ' + $scope.query;
		} else {
			$scope.default_query = $scope.query;
		}
	}
	
	$scope.get_condition_key_display = function(key) {
		return $scope.filter.search_keys[key]['label'];
	};
	generate_in_string = function(cond_value, type){
		var values = cond_value.split(','),
			value = '',
			index = 0,
			count = values.length,
			result = '(';
		
		for(index;index < count;index++){
			if (index != 0){
				result += ',';
			}
			

			value = values[index].trim();
			if (type == 'string') {
				result += '"' + value + '"';
			} else {
				result += value;
			}
		}
		
		return result += ')'
	};
	update_filter = function(){
	    var st =" ",
		    	i = 0,
		    	count = $scope.conditions.length,
		    	cond_op = '',
		    	cond_value = '';
	    
	    if (count == 0){
	        	if ($scope.savedqueries){
	        		$scope.saveQueryButton.prop('disabled', true);
	        	}
	        	
	        	if (!$scope.allow_empty_query) {
	        		$scope.filter_enabled = false;
	        	} else {
	        		$scope.filter_enabled = true;
	        	}
	    } else {
	        	if ($scope.savedqueries) {
	        		if (!$scope.saveQueryButton.prop('usingSavedQuery')) {
	        			$scope.saveQueryButton.prop('disabled', false);
	        		} else {
	        			$scope.saveQueryButton.prop('usingSavedQuery', false);
	        		}
	        	}
	    		$scope.filter_enabled = true;
	    }
	    
	    for(;i<count;i++){
	        if (i>0)
	            st += " and ";   
	        condition = $scope.conditions[i];
	        cond_op = condition.operator;
	        cond_value = condition.value;
		    
	        switch (cond_op){
	        		case 'like':
	        			cond_value ='"' + cond_value + '"';
	        			break;
	        		case 'in':
	        			cond_value = generate_in_string(cond_value, condition.type);
	        			break;
	        		case 'not in':
	        			cond_op = 'nin';
	        			cond_value = generate_in_string(cond_value, condition.type);
	        			break;
	        		case '=':
	        		case '<':
	        		case '<=':
	        		case '>':
	        		case '>=':
	        		default:
	        			//Not Equal
	        			if (encodeURI(cond_op) == '%E2%89%A0') {
		    	        		cond_op = '!=';
		    	        }
	        		
	        			if (condition.type == 'string') {
	        				cond_value ='"' + cond_value + '"'
	        			}
	        		
	        			break;
	        }
	        
	        if (condition.type == 'date') {
	        		cond_value = new Date(cond_value).toISOString();
	        }
	        
	        st+= condition.key+" "+cond_op+" "+cond_value;
	    }
	    $scope.query=st;
	    $scope.checkQueryWildCard();
	};
	$scope.value_click = function() {
		var value = '';
		if ($scope.current_filter && $scope.current_filter.type == 'checkbox'){
			value = $scope.current_filter.value;
			if (value == undefined || value == '' || value == 'false') {
				value = 'true';
			} else {
				value = 'false';
			}
			$scope.current_filter.value = value;
		}
	};
	$scope.add_filter = function(){
		if ($scope.current_filter.type == 'checkbox' && $scope.current_filter.value == undefined) {
			$scope.current_filter.value = 'false';
		}

	    if($scope.current_filter.value && $scope.current_filter.key){
	        $scope.conditions.push($scope.current_filter);
	        $scope.current_filter = {'operator':'='};
	    }
	    update_filter();
	};
	$scope.remove_filter = function(idx){
	    $scope.conditions.splice(idx,1);
	    update_filter();
	};
	$scope.check_type = function(){
	    var type = $scope.filter.filterkey[$scope.current_filter.key]['type'];
	    switch (type) {
			case 'bool':
				type='checkbox';
				$scope.filterop.children().filter(function() {
					return $(this).val() != '=';
				}).hide();
				break;
			case 'html':
				type='string';
			default:
				$scope.filterop.children().show();
				break;
        }
	    $scope.current_filter.type = type;
		$scope.filter_value.prop('type', type);
	    return type;
	};
	$scope.checkQueryWildCard = function(){
		var conditions = $scope.conditions;
		if (conditions.length > 0) {
		    	var index = 0,
		    		count = conditions.length,
		    		enable_filter = true;
		    	
		    	for(;index<count;index++){
		    		if (conditions[index].value == '?'){
		    			enable_filter = false;
		    			break;
		    		}
		    	}
		    	
		    	$scope.filter_enabled = enable_filter;
		}
	};
	$scope.editFilter = function(index, key, operator, value, type){
		$scope.remove_filter(index);
		$scope.current_filter.key = key;
		$scope.current_filter.operator = operator;
		if (type == undefined) {
			type = $scope.check_type();
		}
		$scope.current_filter.type = type;
		if (value == '?'){
			value = '';
		}
		$scope.current_filter.value = value;
	};
	$scope.query_to_conditions = function(query){
	    query = query.trim();
	    $scope.query = query;
	    var conditions = query.split(' and '),
		    	index = 0,
		    	count = conditions.length,
		    	parts = [],
		    	key = '',
		    	operator = '',
		    	value = '';
	    for (;index < count; index++) {
		    	parts = conditions[index].split(' ');
		    	key = parts[0];
		    	operator = parts[1];
		    	value = parts[2];
		    	
		    	if(key!="" && operator!="" && value!=""){
		    			$scope.current_filter = {"key":key,"operator":operator, "value":value};
		    			$scope.add_filter();
		    	}
	    }
	};
}
