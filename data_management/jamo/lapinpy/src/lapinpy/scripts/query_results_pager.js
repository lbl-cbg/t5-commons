function initialize_pager($scope,$http,$location,$modal) {
    $scope.page_enabled = false;
    $scope.current_page = 1;
    $scope.current_page_formatted = 1;
	$scope.page_input = $scope.results_info.find('.result_pager input');
	$scope.page_arrows = $scope.page_input.parent().siblings();

	$scope.prev = function($refElement) {
	    	return $scope.page_change($scope.current_page - 1, $refElement);
	};
	$scope.next = function($refElement) {
	    	return $scope.page_change($scope.current_page + 1, $refElement);
	};
	$scope.first = function($refElement){
	    	return $scope.page_change(1, $refElement);
	};
	$scope.last = function($refElement) {
	    	return $scope.page_change($scope.result_desc.last_page, $refElement);
	};
	$scope.page_change = function(page, $refElement){
        	if (page <= 0) {
        		page = 1;
        	} else if (page >$scope.result_desc.last_page) {
        		page = $scope.result_desc.last_page;
        	}
        	
	    	return $scope.dataChange(page, false, $refElement);
	};
	
	angular.element(document).find('body')[0].addEventListener('keydown', function(ev){
	     var pressed = ev.which,
		 	 $this = $(this);
	     if(document.activeElement.localName=="body") {
	         if(pressed== 39 && !$scope.page_arrows.filter('.next_func').hasClass('disabled'))
	             $scope.next($this);
	         else if (pressed== 37 && !!$scope.page_arrows.filter('.prev_func').hasClass('disabled'))
	             $scope.prev($this);
	     }	
	});
	
	$scope.results_info.off('change', '.page input').on('change', '.page input', function(){
		var $this = $(this),
			page = $this.val();
		
		if (page != $scope.current_page && $this.attr('disabled') == undefined) {
			return $scope.page_change(Number(page.replace(/,/g, '')), $this);
		}
	}).off('click', '.result_pager div.page_func').on('click', '.result_pager div.page_func', function(){
		var $this = $(this),
			id = $this.attr('class').replace('page_func', '').trim(),
			disabled = $this.hasClass('disabled'); 
		
		if (!disabled){
			switch (id){
				case 'first_func':
					$scope.first($this);
					break;
				case 'prev_func':
					$scope.prev($this);
					break;
				case 'next_func':
					$scope.next($this);
					break;
				case 'last_func':
					$scope.last($this);
					break;
			}
		}
	});
}

function updatePaging($scope) {
    	var page_enabled = $scope.result_desc.total  > $scope.record_per_page;
    	
    	if (page_enabled){
    		$scope.page_input.removeAttr('disabled', 'disabled');
    		if ($scope.current_page != $scope.result_desc.last_page) {
    			$scope.page_arrows.filter('.next_func').removeClass('disabled');
    		} else {
    			$scope.page_arrows.filter('.next_func').addClass('disabled');
    		}
    		
    		if ($scope.current_page > 1) {
    			$scope.page_arrows.filter('.prev_func').removeClass('disabled');
    		} else {
    			$scope.page_arrows.filter('.prev_func').addClass('disabled');
    		}
    		
    		if ($scope.current_page > 2) {
    			$scope.page_arrows.filter('.first_func').removeClass('disabled');
    		} else {
    			$scope.page_arrows.filter('.first_func').addClass('disabled');
    		}

			if ($scope.current_page + 2 <= $scope.result_desc.last_page) {
				$scope.page_arrows.filter('.last_func').removeClass('disabled');
			} else {
				$scope.page_arrows.filter('.last_func').addClass('disabled');
			}
    	} else {
    		$scope.page_input.val(1);
    		$scope.page_input.attr('disabled', 'disabled');
    		$scope.page_arrows.addClass('disabled');
    	}

    	$scope.filter_enabled=true;
};