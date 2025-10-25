// public/cookies.js
// Decorative floating cookie sprites. Place a cookie image at /images/cookie.png.
// If you don't want the cookie animation, remove this script and the .cookie-container element.

(function () {
  const COUNT = 10;
  const container = document.querySelector('.cookie-container');
  if (!container) return;
  const cookieUrl = '/assets/cookie.png';

  function rand(min, max) { return Math.random() * (max - min) + min; }

  for (let i = 0; i < COUNT; i++) {
    const el = document.createElement('div');
    el.className = 'cookie';
    const size = Math.round(rand(40, 110));
    el.style.width = `${size}px`;
    el.style.height = `${size}px`;
    el.style.left = `${rand(-10, 90)}%`;
    el.style.top = `${rand(-10, 90)}%`;
    el.style.backgroundImage = `url("${cookieUrl}")`;
    el.style.backgroundSize = 'contain';
    el.style.backgroundRepeat = 'no-repeat';
    el.style.opacity = `${rand(0.6, 0.95)}`;
    const dur = rand(12, 30);
    const delay = rand(0, 8);
    el.style.animation = `float-drift ${dur}s ease-in-out ${delay}s infinite`;
    el.style.position = 'absolute';
    el.style.pointerEvents = 'none';
    el.style.zIndex = '0';
    container.appendChild(el);
  }
})();