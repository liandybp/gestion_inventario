// Login: casilla "Mostrar contraseña".
const cb = document.getElementById('login-show-password');
const pw = document.getElementById('login-password');
if (cb && pw) {
  cb.addEventListener('change', () => {
    pw.type = cb.checked ? 'text' : 'password';
  });
}
