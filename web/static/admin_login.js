/**
 * static/admin_login.js — Admin login page JS
 * (renamed from login.js for consistency with admin_login.html / admin_login.css)
 */

function togglePw() {
  const input = document.getElementById('password');
  const btn   = document.getElementById('pw-toggle');
  input.type  = input.type === 'password' ? 'text' : 'password';
  btn.textContent = input.type === 'password' ? '👁' : '🙈';
}

document.getElementById('password').addEventListener('input', () => {
  const err = document.querySelector('.login-error');
  if (err) err.style.opacity = '.5';
});
