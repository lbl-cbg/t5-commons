function initialize_filter_savedqueries($scope,$http,$location,$modal) {
	$scope.saved_filters = $('.saved_query ul');
	saved_queries = data['saved_queries'];
	$scope.saved_queries = saved_queries['queries'];
	$scope.saved_query_page = saved_queries['page'];
    $scope.saveQueryModel = null;
    $scope.user = data['user'];
    $scope.save_query_details = function(){
		$scope.query_to_save = $scope.query.replace(/"/g, '');
    		$scope.saveQueryModel = $modal({scope:$scope, template:"/api/core/htmltemplate/save_query.tpl.html"});
    };
    
    $scope.saved_filters.off('click', 'li > a').on('click', 'li > a', function(){
    		$scope.useSavedQuery($scope.saved_queries[$(this).parent().index()].query);
    }).off('click', 'li > div').on('click', 'li > div', function(){
    		var $li = $(this).parent(),
    			index = $li.index(),
    			query = $scope.saved_queries[index],
    			name = query.name;
	    	$http.post('/api/core/userquery',{'query':query.query, 
												'name': name,
												'description': query.description,
												'remove_query': true,
												'page':$scope.saved_query_page,
												'user':$scope.user}
	    	).success(function(){
				$scope.saved_queries.splice(index, 1);
				$li.remove();
	    	}).error(function(data) {
				$scope.error_code = data.error_code;
				$scope.error_message = data.errors[0];
				$modal({scope:$scope, template:"/api/core/htmltemplate/error_message.tpl.html"});
			});
    });
    
    $scope.useSavedQuery = function(query){
	    	$scope.saveQueryButton.prop('disabled', true).prop('usingSavedQuery', true);
	    	if (query.indexOf('?') >= 0){
	    		$scope.filter_enabled = false;
	    	} else {
	    		$scope.filter_enabled = true;
	    	}
	    	$scope.conditions = [];
        $scope.query_to_conditions(query);
        $scope.dataChange(1, true);
    };
    $scope.saveQuery = function(){
        var querys = document.getElementById('modal_query').value,
	        	queryn = document.getElementById('modal_query_name').value,
	        	queryd = document.getElementById('modal_query_desc').value,
	        	index = 0,
	        	queries = $scope.saved_queries,
	        	count = queries.length;
        //Check to ensure that the name/user/page combination is unqiue.
        for (;index < count;index++) {
	        	if (queries[index].name == queryn) {
	        		alert('Queries must have a unique user/name/page combination.\n\nThe query name must be changed to be unique for this page before the query can be saved.');
	        		return;
	        	}
        }
        //If the name/page/user combination is unique, save the query.
        $http.post('/api/core/userquery',{'query':querys, 
	        	'name': queryn, 
	        	'description': queryd, 
	        	'remove_query':false,
	        	'page':$scope.saved_query_page,
	        	'user':$scope.user}
        ).success(function(data, status, headers, config) {
            	$scope.saved_queries.push({'name':queryn, 'query':querys, 'description':queryd});
            $scope.saved_filters.append('<li><div class="delete" title="Delete">X</div><a href="#" title="' + queryd + '">' + queryn + '</a></li>');
            	$scope.saveQueryButton.prop('disabled', true);
            $scope.saveQueryModel.hide();
        }).error(function(data) {
   			$scope.error_code = data.error_code;
   			$scope.error_message = data.errors[0];
   			$modal({scope:$scope, template:"/api/core/htmltemplate/error_message.tpl.html"});
       	}); 
    };
    
    $scope.saveQueryButton = $('.search_filter .actions .save_query');
}
