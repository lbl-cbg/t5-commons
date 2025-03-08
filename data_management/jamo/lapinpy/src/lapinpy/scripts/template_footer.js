$(".selection").chosen();

$(document).ready(function() {
    var $mainNav = $('#menubar2 > .main > ul');

    $mainNav.off('click.menuopen', 'li').on('click.menuopen', 'li', function(e) {
        var $this = $(this),
            $child = $this.children('ul'),
            $menu = null;

        if ($child.length != 0) {
            e.preventDefault();

            if ($this.closest('ul').parent().is('li')) {
                e.stopImmediatePropagation();
            }

            if ($child.css('visibility') == 'visible') {
                if (!$this.hasClass('opened')) {
                    $menu = openMenu($this, $child);
                } else {
                    $menu = $this;
                }
            } else {
                $menu = openMenu($this, $child);
            }

            closeMenus($menu);
        } else {
            e.stopImmediatePropagation();
        }
    });

    $(window).on('click', function(e) {
        if ($(e.target).closest('nav.main').length == 0 && $mainNav.children('.opened').length != 0) {
            closeMenus($mainNav.children('.opened'));
        }
    });

    function openMenu($menu, $child) {
        var $primary = $child.closest('.primary'),
            $close = null,
            process = true,
            $arrow = $menu.children('.arrow');

        arrowToggle($arrow);

        while (process) {
            $menu = $child.removeClass('hideElement').parent('li');
            $child = $menu.addClass('opened new').parent('ul');
            $arrow = $menu.children('.arrow');

            if ($arrow.hasClass('right') || $arrow.hasClass('down')) {
                arrowToggle($arrow);
            }

            process = $child.length > 0 && $child.hasClass('hideElement');
        }

        $close = $primary.find('.opened:not(.new)').filter(function() {
            return $(this).siblings('.opened.new').length > 0;
        });

        if ($close.length == 0) {
            $close = $mainNav.find('.opened:not(.new)');
        }

        $primary.find('.new').removeClass('new');

        return $close.first();
    }

    function closeMenus($menu) {
        var $arrow = $menu.children('.arrow');

        $menu.removeClass('opened').find('ul').addClass('hideElement');
        arrowToggle($arrow);

        $menu.find('.opened').each(function() {
            arrowToggle($(this).removeClass('opened').children('.arrow'));
        });
    }

    function arrowToggle($arrow) {
        if ($arrow.hasClass('up')) {
            $arrow.removeClass('up').addClass('down');
        } else if ($arrow.hasClass('down')) {
            $arrow.removeClass('down').addClass('up');
        } else if ($arrow.hasClass('right')) {
            $arrow.removeClass('right').addClass('left');
        } else if ($arrow.hasClass('left')) {
            $arrow.removeClass('left').addClass('right');
        }
    }
});