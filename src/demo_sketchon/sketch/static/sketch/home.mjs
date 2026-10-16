import {initAccountState} from './account-state.mjs';
import {createTracker} from './events.mjs';

const initial = JSON.parse(document.getElementById('initial-data').textContent);
const visitor = JSON.parse(document.getElementById('visitor-data').textContent);
const {record, flushEvents} = createTracker(initial.id, visitor);
document.getElementById('start-sketch').addEventListener('click', () => {
    record('button.clicked', {button: 'start-sketch', page: 'home'});
    void flushEvents();
});
void flushEvents();

initAccountState({projectId:initial.id});
