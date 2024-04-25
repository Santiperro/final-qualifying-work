window.onload = function() {
    let today = new Date().toISOString().split('T')[0];
    document.getElementById('startDate').max = today;
    document.getElementById('endDate').max = today;
    document.getElementById('startDate').value = today;
    document.getElementById('endDate').value = today;
    validateNumbers('minParticipants');
    validateNumbers('minStars');
    validateNumbers('numRepos');
}

function validateDates(changedInputId) {
    let startDate = document.getElementById('startDate');
    let endDate = document.getElementById('endDate');
    let notification = document.getElementById('notification');
    startDate.classList.remove('error');
    endDate.classList.remove('error');
    if (new Date(startDate.value) > new Date(endDate.value)) {
        document.getElementById(changedInputId).classList.add('error');
        notification.textContent = 'Начальная дата не может быть позже конечной даты';
        notification.style.display = 'block';
    } else {
        notification.style.display = 'none';
    }
}

function validateNumbers(inputId) {
    let input = document.getElementById(inputId);
    let notification = document.getElementById('notification');
    if (input.value < input.min || input.value > input.max) {
        input.classList.add('error');
        notification.textContent = 'Введенное значение выходит за допустимые пределы';
        notification.style.display = 'block';
    } else {
        input.classList.remove('error');
        notification.style.display = 'none';
    }
}