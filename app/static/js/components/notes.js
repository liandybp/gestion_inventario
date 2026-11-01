// Notas reutilizables: los <input name="note"> ofrecen (datalist) las últimas 20 notas
// guardadas en localStorage; al enviar el formulario se guarda la nota usada.
import { onContent } from '../core/content.js';

const NOTES_KEY = 'reusable_notes';

function saveReusableNote(note) {
  if (!note || !note.trim()) return;
  const trimmed = note.trim();
  try {
    let notes = JSON.parse(localStorage.getItem(NOTES_KEY) || '[]');
    if (!notes.includes(trimmed)) {
      notes.unshift(trimmed);
      notes = notes.slice(0, 20);
      localStorage.setItem(NOTES_KEY, JSON.stringify(notes));
    }
  } catch (e) {}
}

function getReusableNotes() {
  try {
    return JSON.parse(localStorage.getItem(NOTES_KEY) || '[]');
  } catch (e) {
    return [];
  }
}

function enhanceNoteInputs(root) {
  const noteInputs = (root || document).querySelectorAll('input[name="note"]');
  noteInputs.forEach((input) => {
    if (input.hasAttribute('data-note-enhanced')) return;
    input.setAttribute('data-note-enhanced', 'true');

    let datalist = input.list;
    if (!datalist) {
      const listId = 'reusable-notes-list-' + Math.random().toString(36).substr(2, 9);
      datalist = document.createElement('datalist');
      datalist.id = listId;
      input.setAttribute('list', listId);
      input.parentElement.appendChild(datalist);
    }

    const notes = getReusableNotes();
    datalist.innerHTML = '';
    notes.forEach((note) => {
      const option = document.createElement('option');
      option.value = note;
      datalist.appendChild(option);
    });

    const form = input.closest('form');
    if (form && !form.hasAttribute('data-note-save-listener')) {
      form.setAttribute('data-note-save-listener', 'true');
      form.addEventListener('submit', () => {
        const noteVal = input.value;
        if (noteVal) saveReusableNote(noteVal);
      });
    }
  });
}

export function init() {
  onContent(enhanceNoteInputs);
}
