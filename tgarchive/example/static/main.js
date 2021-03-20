(function() {
	// Hide the open burger menu when clicking nav links. 
	const burger = document.querySelector("#burger");
	document.querySelectorAll(".timeline a, .dayline a").forEach((e) => {
		e.onclick = () => {
			burger.checked = false;
		};
	});

	// Change page anchor on scrolling past days.
	let is = null;
	document.onscroll = () => {
		if (is) {
			window.clearTimeout(is);
		}
		is = window.setTimeout(() => {
			const days = document.querySelectorAll(".messages .day");
			let lastID = days[0].id;
			days.forEach((el) => {
				if (el.getBoundingClientRect().top < 10) {
					lastID = el.id;
				} else {
					return false;
				}
			});

			if (lastID) {
				history.replaceState({}, "", `#${lastID}`);
				let q = document.querySelector(".dayline .selected")
				if (q) {
					q.classList.remove("selected");
				}
				document.querySelector(`.dayline .day-${lastID}`).classList.add("selected");
			}
		}, 100);
	};
})();
