window.onload = function() {
    let today = new Date().toISOString().split('T')[0];
    document.getElementById('startDate').max = today;
    document.getElementById('endDate').max = today;
    document.getElementById('startDate').value = today;
    document.getElementById('endDate').value = today;
    validateDates('startDate');
    validateDates('endDate');
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
    let inputValue = Number(input.value);
    let min = Number(input.min);
    let max = Number(input.max);
    let labelElement = document.querySelector(`label[for=${inputId}]`);
    let notification = document.getElementById('notification');
    if (labelElement) {
        let label = labelElement.textContent;
        if (inputValue < min || inputValue > max) {
            input.classList.add('error');
            notification.textContent = `${label} должно быть в промежутке от ${min} до ${max}. Текущее значение: ${inputValue}`;
            notification.style.display = 'block';
        } else {
            input.classList.remove('error');
            notification.style.display = 'none';
        }
    } else {
        console.error(`Label for ${inputId} not found.`);
    }
}

// load data params table

function toggleDivisionType(element, name) {
    if (element.textContent === 'Децили') {
        element.textContent = 'Квартили';
    } else if (element.textContent === 'Квартили') {
        element.textContent = 'Децили';
    }
}

function load_data_submit() {
    var selections = {};
    // Заполняем selections перед отправкой данных
    var rows = document.querySelectorAll('tbody tr');
    rows.forEach(function(row) {
        var name = row.querySelector('td').textContent;
        var divisionTypeElement = row.querySelector('.clickable');
        var divisionType = divisionTypeElement.textContent === '-' ? null : divisionTypeElement.textContent;
        var isChecked = row.querySelector('input[type="checkbox"]').checked;
        if (isChecked) {
            selections[name] = divisionType;
        }
    });

    // Собираем значения полей ввода
    let data = {
        startDate: document.getElementById('startDate').value,
        endDate: document.getElementById('endDate').value,
        minParticipants: document.getElementById('minParticipants').value,
        minStars: document.getElementById('minStars').value,
        numRepos: document.getElementById('numRepos').value,
        isNewRepos: document.querySelector('input[name="choice"]:checked').parentElement.textContent.trim() === 'Новые репозитории',
        items: selections,
        note: document.querySelector('input[type="text"]').value
    };

    // Отправляем запрос на сервер
    fetch('/load-data-submit', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
    .then((response) => response.json())
    .then((data) => {
        console.log('Success:', data);
    })
    .catch((error) => {
        console.error('Error:', error);
    });
}

// samples table

let selectedRows = [];

function toggleSelection(element, id) {
    if (element.checked) {
        if (!selectedRows.includes(id)) {
            selectedRows.push(id);
        }
    } else {
        const index = selectedRows.indexOf(id);
        if (index > -1) {
            selectedRows.splice(index, 1);
        }
    }
    console.log(selectedRows);
}

function sendDataToServer() {
    // Здесь вы можете добавить код для отправки данных на сервер
    console.log("Отправка данных на сервер: ", selectedRows);
}