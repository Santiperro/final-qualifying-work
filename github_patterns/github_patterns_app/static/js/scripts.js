window.onload = function() {
    if (window.location.pathname != '/request-data') {
        return;
    }
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
    let errorDiv = document.getElementById('errorDiv');
    startDate.classList.remove('error');
    endDate.classList.remove('error');
    
    if (!isValidDate(startDate.value)) {
        startDate.classList.add('error');
        errorDiv.textContent = 'Начальная дата некорректна';
        errorDiv.style.display = 'block';
        return;
    }
    
    if (!isValidDate(endDate.value)) {
        endDate.classList.add('error');
        errorDiv.textContent = 'Конечная дата некорректна';
        errorDiv.style.display = 'block';
        return;
    }
    
    if (new Date(startDate.value) > new Date(endDate.value)) {
        document.getElementById(changedInputId).classList.add('error');
        errorDiv.textContent = 'Начальная дата не может быть позже конечной даты';
        errorDiv.style.display = 'block';
    } else {
        errorDiv.style.display = 'none';
    }
}

function isValidDate(dateString) {
    let date = new Date(dateString);
    return !isNaN(date.getTime());
}

function validateNumbers(inputId) {
    let input = document.getElementById(inputId);
    let inputValue = Number(input.value);
    let min = Number(input.min);
    let max = Number(input.max);
    let labelElement = document.querySelector(`label[for=${inputId}]`);
    let errorDiv = document.getElementById('errorDiv');
    if (labelElement) {
        let label = labelElement.textContent;
        if (!input.value) {
            input.classList.add('error');
            errorDiv.textContent = `${label} не может быть пустым`;
            errorDiv.style.display = 'block';
        } else if (inputValue < min || inputValue > max) {
            input.classList.add('error');
            errorDiv.textContent = `${label} должно быть в промежутке от ${min} до ${max}. Текущее значение: ${inputValue}`;
            errorDiv.style.display = 'block';
        } else {
            input.classList.remove('error');
            errorDiv.style.display = 'none';
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
    validateDates('startDate');
    validateDates('endDate');
    validateNumbers('minParticipants');
    validateNumbers('minStars');
    validateNumbers('numRepos');
    let errorElements = document.querySelectorAll('.error');
    if (errorElements.length > 0) {
        console.log(errorElements)
        return;
    }
    var selections = {};
    // Заполняем selections перед отправкой данных
    var rows = document.querySelectorAll('tbody tr');
    rows.forEach(function(row) {
        var name = row.querySelector('td').textContent;
        var divisionTypeElement = row.querySelector('.clickable');
        var divisionTypeText = divisionTypeElement.textContent;
        var divisionType;
        if (divisionTypeText === 'Децили') {
            divisionType = 'dec';
        } else if (divisionTypeText === 'Квартили') {
            divisionType = 'qua';
        } else {
            divisionType = null;
        }
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

    let errorDiv = document.getElementById('errorDiv');
    let notification = document.getElementById('notification');

    // Отправляем запрос на сервер
    fetch('/load-data-submit', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
    .then((response) => {
        if (!response.ok) {
            return response.json().then(json => { throw json });
        }
        errorDiv.style.display = 'none';
        return response.json();
    })
    .then((data) => {
        console.log('Success:', data);
        notification.textContent = "Данные успешно сохранены";
        notification.style.display = 'block';
        
    })
    .catch((error) => {
        errorDiv.textContent = error.Error;
        errorDiv.style.display = 'block';
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

function find_patterns_submit() {
    validateNumbers('antecedent');
    validateNumbers('consequent');
    validateNumbers('minsup');
    validateNumbers('minconf');
    validateNumbers('lift');
    let errorElements = document.querySelectorAll('.error');
    if (errorElements.length > 0) {
        console.log(errorElements)
        return;
    }

    let ids = selectedRows;

    if (ids.length === 0) {
        let errorDiv = document.getElementById('errorDiv');
        errorDiv.textContent = `Выберите минимум одну выборку`;
        errorDiv.style.display = 'block';
        return;
    }

    console.log(ids)

    // Собираем значения полей ввода
    let antecedent = document.getElementById('antecedent').value;
    let consequent = document.getElementById('consequent').value;
    let minsup = document.getElementById('minsup').value;
    let minconf = document.getElementById('minconf').value;
    let lift = document.getElementById('lift').value;

    // Формируем данные для отправки на сервер
    let data = {
        antecedent: antecedent,
        consequent: consequent,
        minsup: minsup,
        minconf: minconf,
        lift: lift,
        ids: ids
    };

    // Отправляем запрос на сервер
    fetch('/find-patterns-submit', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
    .then((response) => {
        if (!response.ok) {
            return response.json().then(json => { throw json });
        }
        errorDiv.style.display = 'none';
        return response.json();
    })
    .then((data) => {
        console.log('Success:', data);
        // Здесь мы добавляем код для отображения таблицы
        let patternsTable = document.getElementById('PatternsTable');
        patternsTable.style.display = 'block'; // Показываем таблицу
        let tbody = patternsTable.getElementsByTagName('tbody')[0];
        document.getElementById('download').style.display = 'block';
        tbody.innerHTML = ''; // Очищаем таблицу
        for (let pattern of data) {
            let row = tbody.insertRow();
            row.insertCell().innerText = pattern.antecedents;
            row.insertCell().innerText = pattern.consequents;
            row.insertCell().innerText = pattern.support;
            row.insertCell().innerText = pattern.confidence;
            row.insertCell().innerText = pattern.lift   ;
        }
    })
    .catch((error) => {
        errorDiv.textContent = error.Error;
        errorDiv.style.display = 'block';
        console.error('Error:', error);
    });
}


document.querySelectorAll('.delete-button').forEach(button => {
    button.addEventListener('click', function() {
        const id = this.dataset.id;
        fetch('/delete-sample/' + id, {
            method: 'DELETE',
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                document.getElementById(id).remove();
            } else {
                console.error('Error:', data.error);
            }
        });
    });
});

document.getElementById('download').addEventListener('click', function() {
    var table = document.getElementById('PatternsTable');
    var wb = XLSX.utils.table_to_book(table);
    XLSX.writeFile(wb, 'data.xlsx');
});