function validateEmail(email) { 
	var re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
	return re.test(email);
}

$(document).ready(function () {

	// Smooth Wheel
	$("body").smoothWheel();

	// Stop resize div #intro on mobile devices
	if($(window).width() < 750) {
		$('#intro').css('height', ($(window).height() + 60) +'px');
	}
	var height = $(window).height() + 60;
	$(window).resize(function(){
		if($(window).width() < 750) {
	        $('#intro').css('height', height +'px');
		} else {
			$("#intro").removeAttr("style");
		}
	});

	// WOW initizalition
	var wow = new WOW({
    	mobile: false
    });
	wow.init();

	// ScrollIt configuration
	$(function () {
		$.scrollIt({
			upKey: 38,             // key code to navigate to the next section
			downKey: 40,           // key code to navigate to the previous section
			easing: 'linear',		// The easing function for animation
			scrollTime: 500			// How long (in ms) the animation takes
		});
	});

	// Modernizr to handle touch event on images.
	if (Modernizr.touch) {
		// Show the close overlay button
		$(".close-overlay").removeClass("hidden");
		// Handle the adding of hover class when clicked
		$(".img").click(function (e) {
			if (!$(this).hasClass("hover")) {
				$(this).addClass("hover");
			}
		});
		// Handle the closing of the overlay
		$(".close-overlay").click(function (e) {
			e.preventDefault();
			e.stopPropagation();
			if ($(this).closest(".img").hasClass("hover")) {
				$(this).closest(".img").removeClass("hover");
			}
		});
	} else {
		// Handle the mouseenter functionality
		$(".img").mouseenter(function () {
			$(this).addClass("hover");
		})
				// Handle the mouseleave functionality
				.mouseleave(function () {
					$(this).removeClass("hover");
				});
	}

	// Scroll Top
    $('#scrollTop').click(function () {
        $("html, body").animate({scrollTop: 0}, 600);
        return false;
    });

});