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

    if (Object.keys(selections).length < 3) {
        errorDiv.textContent = 'Выберите минимум 3 элемента транзакции';
        errorDiv.style.display = 'block';
        return;
    }


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

function getPatternWordEnding(number) {
    let lastDigit = number % 10;
    let lastTwoDigits = number % 100;
    if (lastDigit === 1 && lastTwoDigits !== 11) {
        return 'шаблон';
    } else if ([2, 3, 4].includes(lastDigit) && ![12, 13, 14].includes(lastTwoDigits)) {
        return 'шаблона';
    } else {
        return 'шаблонов';
    }
}

function find_patterns_submit() {
    document.getElementById('filterAntecedent').style.display = 'none';
    document.getElementById('filterConsequent').style.display = 'none';
    document.getElementById('TableContainer').style.display = 'none';
    document.getElementById('download').style.display = 'none';
    document.getElementById('QuantilesTableContainer').style.display = 'none';
    document.getElementById('toggleQuantilesBtn').style.display = 'none';
    document.getElementById('TableContainerName').style.display = 'none';
    let errorDiv = document.getElementById('errorDiv');
    errorDiv.style.display = 'none';

    validateNumbers('antecedent');
    validateNumbers('consequent');
    validateNumbers('minsup');
    validateNumbers('minconf');
    validateNumbers('lift');
    let errorElements = document.querySelectorAll('.error');
    if (errorElements.length > 0) {
        console.log(errorElements);
        return; 
    }

    let ids = selectedRows;
    if (ids.length === 0) {
        errorDiv.textContent = 'Выберите минимум одну выборку';
        errorDiv.style.display = 'block';
        return; 
    }

    let antecedent = document.getElementById('antecedent').value;
    let consequent = document.getElementById('consequent').value;
    let minsup = document.getElementById('minsup').value;
    let minconf = document.getElementById('minconf').value;
    let lift = document.getElementById('lift').value;
    let data = {
        antecedent: antecedent,
        consequent: consequent,
        minsup: minsup,
        minconf: minconf,
        lift: lift,
        ids: ids
    };

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
        document.getElementById('filterAntecedent').style.display = 'inline-block';
        document.getElementById('filterConsequent').style.display = 'inline-block';
        document.getElementById('TableContainer').style.display = 'block';
        document.getElementById('download').style.display = 'block';
        document.getElementById('toggleQuantilesBtn').style.display = 'block';
        document.getElementById('TableContainerName').style.display = 'block';
        
        let tbody = document.getElementById('PatternsTable').getElementsByTagName('tbody')[0];
        tbody.innerHTML = ''; 

        if (Array.isArray(data.patterns)) {
            let patternWordEnding = getPatternWordEnding(data.patterns.length);
            document.getElementById('TableContainerName').textContent = `Найдено ${data.patterns.length} ${patternWordEnding}:`;

            data.patterns.forEach(pattern => {
                let row = tbody.insertRow();
                row.insertCell().innerText = pattern.antecedents;
                row.insertCell().innerText = pattern.consequents;
                row.insertCell().innerText = pattern.support;
                row.insertCell().innerText = pattern.confidence;
                row.insertCell().innerText = pattern.lift;
            });
        }

        if (Array.isArray(data.quartiles) && data.quartiles.length > 0) {
            populateTable('QuartilesTable', 'QuartilesTableHeader', data.quartiles);
        }

        if (Array.isArray(data.deciles) && data.deciles.length > 0) {
            populateTable('DecilesTable', 'DecilesTableHeader', data.deciles);
        }

        document.getElementById('QuantilesTableContainer').style.display = 'block';
    })
    .catch((error) => {
        errorDiv.textContent = error.Error;
        errorDiv.style.display = 'block';
        console.error('Error:', error);
    });
}

function toggleQuantiles() {
    let tablesDiv = document.getElementById('QuantilesTables');
    let toggleIcon = document.getElementById('toggleIcon');
    let toggleText = document.getElementById('toggleText');
    if (tablesDiv.style.display === 'none') {
        tablesDiv.style.display = 'block';
        toggleIcon.textContent = '▼';
        toggleText.textContent = 'Скрыть таблицу квантилей и децилей'; 
    } else {
        tablesDiv.style.display = 'none';
        toggleIcon.textContent = '⯈';
        toggleText.textContent = 'Показать таблицу квантилей и децилей'; 
    }
}

function populateTable(tableId, headerId, data) {
    let tableHeader = document.getElementById(headerId);
    let tableBody = document.getElementById(tableId).getElementsByTagName('tbody')[0];
    tableBody.innerHTML = ''; 

    if (data.length > 0) {
        let headers = Object.keys(data[0]);
        tableHeader.innerHTML = '';
        headers.forEach(header => {
            let th = document.createElement('th');
            th.innerText = header;
            th.style.textAlign = 'center';
            tableHeader.appendChild(th);
        });

        data.forEach(rowData => {
            let row = tableBody.insertRow();
            headers.forEach(header => {
                let cell = row.insertCell();
                cell.innerText = rowData[header];
                cell.style.textAlign = 'right';
            });
            row.cells[0].style.textAlign = 'center';
        });
    }
}
/**
 * The function `toggleQuantilesTable` toggles the visibility of a table container
 * and updates a toggle icon accordingly.
 */
// function toggleQuantilesTable() {
//     let tableContainer = document.getElementById('QuantilesTableContainer');
//     let toggleIcon = document.getElementById('toggleIcon');
//     if (tableContainer.style.display === 'none') {
//         tableContainer.style.display = 'block';
//         toggleIcon.textContent = '▲';
//     } else {
//         tableContainer.style.display = 'none';
//         toggleIcon.textContent = '▼';
//     }
// }


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

document.addEventListener('DOMContentLoaded', () => {
    document.querySelector('.new-sample-button').addEventListener('click', () => {
        window.location.href = '/request-data';
    });
});

document.getElementById('download').addEventListener('click', function() {
    var table = document.getElementById('PatternsTable');
    var wb = XLSX.utils.table_to_book(table);
    XLSX.writeFile(wb, 'data.xlsx');
});

document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('PatternsTable').style.display = 'table';
});

function filterTable(filterId, colIndex) {
    const input = document.getElementById(filterId);
    const filter = input.value.toLowerCase();
    const table = document.getElementById('PatternsTable');
    const tr = table.getElementsByTagName('tr');

    for (let i = 1; i < tr.length; i++) {
        const td = tr[i].getElementsByTagName('td')[colIndex];
        if (td) {
            const txtValue = td.textContent || td.innerText;
            tr[i].style.display = txtValue.toLowerCase().indexOf(filter) > -1 ? '' : 'none';
        }
    }
}

function sortTable(colIndex, ascending) {
    const table = document.getElementById('PatternsTable');
    const tbody = table.getElementsByTagName('tbody')[0];
    const rows = Array.from(tbody.getElementsByTagName('tr'));

    rows.sort((a, b) => {
        const aText = a.getElementsByTagName('td')[colIndex].textContent;
        const bText = b.getElementsByTagName('td')[colIndex].textContent;
        const aValue = parseFloat(aText.replace(',', '.')) || 0;
        const bValue = parseFloat(bText.replace(',', '.')) || 0;

        return ascending ? aValue - bValue : bValue - aValue;
    });

    rows.forEach(row => tbody.appendChild(row));
}
