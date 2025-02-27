function formatInteger(value){
	var formattedValue = '';
	value = value.toString();
	if (value.length > 3){
    	var digits = value.split('').reverse(),
    		index = 0,
    		count = digits.length;
    	for(index;index < count;index++){
    		if (index % 3 == 0 && index != 0) {
    			formattedValue = ',' + formattedValue;
    		}
    		
    		formattedValue = digits[index] + formattedValue;
    	}
	} else {
		formattedValue = value;
	}
	
	return formattedValue;
}

function formatFloat(value, decimalDigits) {
	var components = value.toString().split('.'),
		formattedValue = formatInteger(components[0]);
	
	if (decimalDigits == undefined || decimalDigits == null){
		decimalDigits = 3;
	}
	
	if (components.length > 1) {
		formattedValue += '.' + components[1].substring(0,decimalDigits);
	} 
	
	return formattedValue;
}

function removeNumberFormat(value){
	return value.replace(/,/g, '')
}