function SearchCtrl($scope,$http){
    $scope.current_results=[{selected:false,desc:'fist',file_name:'file_name',added_date:'10:04pm'}];
    $scope.prev_records = [];
    $scope.stored_amount=0;
    $scope.prev_enabled = false;
    $scope.next_enabled = false;
    $scope.result_desc = { start:0, end:0, record_count:0};
    $scope.search = function(){
        $http.post('/api/metadata/search',{query:$scope.query}).success(
            function(data, status, headers, config) {
                $scope.current_results=data.records;
                $scope.prev_records =  []
                $scope.cursor_id=data.cursor_id
                $scope.result_desc = { start:data.start, end:data.end, record_count:data.record_count} 
                $scope.prev_records = $scope.prev_records.concat($scope.current_results);
                $scope.stored_amount=data.end;
                $scope.next_enabled = data.end<data.record_count;
            });
    };
    $scope.next = function(){
        if(!$scope.next_enabled){
            return;
        }
        if($scope.stored_amount> $scope.result_desc.start+50){
           $scope.result_desc.start +=50;
           $scope.result_desc.end +=50;
           $scope.current_results = $scope.prev_records.slice($scope.result_desc.start-1,$scope.result_desc.end);
           $scope.next_enabled = data.end<data.record_count;
        }else if($scope.result_desc.end<$scope.result_desc.record_count){
            $http.post('/api/metadata/search/'+$scope.cursor_id,{query:$scope.query}).success(
                function(data, status, headers, config) {
                    $scope.current_results=data.records;
                    $scope.result_desc = { start:data.start, end:data.end, record_count:data.record_count} 
                    $scope.prev_records = $scope.prev_records.concat($scope.current_results);
                    $scope.stored_amount+=50;
                    $scope.next_enabled = data.end<data.record_count;
                    $scope.prev_enabled = true;
                });
        }
    };
    $scope.prev = function(){
        
        if($scope.result_desc.start>1 && $scope.prev_enabled){
            $scope.result_desc.start -=50; 
            $scope.result_desc.end -=50;
            $scope.current_results = $scope.prev_records.slice($scope.result_desc.start-1,$scope.result_desc.end);
        }
        $scope.prev_enabled = $scope.result_desc.start>1;

    };

};
