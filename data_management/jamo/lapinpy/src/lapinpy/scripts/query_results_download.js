function initialize_download($scope,$http,$location,$modal) {
	$scope.download_button = $scope.result_info.find('.download_results > i');
	$scope.download_link = '/api/file/download/csv/' + $scope.download+ '?download=true';
	$scope.download_query = function(event, thiss, that){
		if ($scope.download_button.hasClass('disabled')) {
			return;
		} else {
			$scope.download_button.addClass('disabled');
			
			$http.get($scope.download_link+'&return_count=' + $scope.result_desc.total + '&query='+encodeURIComponent($scope.query))
				 .success(function(response) {
						var de = document.createElement('a');				 
	
						de.href = 'data:attachment/csv,' + response;
						de.target = 'blank';
						de.download = $scope.download.substr($scope.download.lastIndexOf('/')) + '.csv';
						de.click();
						$scope.download_button.removeClass('disabled');
				 })
				 .error(function(response) {
					 $scope.download_button.removeClass('disabled');
				 });
		}
	};
}
