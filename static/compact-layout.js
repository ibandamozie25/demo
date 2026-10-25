document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.school-workspace table').forEach(table => {
    // Preserve report layouts and interactive form tables.
    if (table.closest('.page, .modal, [data-table-scroll="off"]') ||
        table.matches('.report') ||
        table.querySelector('input, select, textarea') ||
        !table.tBodies.length ||
        Array.from(table.tBodies).reduce((n,b) => n+b.rows.length,0)<8) return;
    let wrapper=table.closest('.table-responsive');
    if (!wrapper) {
      wrapper=document.createElement('div');
      wrapper.className='table-responsive';
      table.before(wrapper);wrapper.append(table);
    }
    wrapper.classList.add('school-table-scroll');
    wrapper.tabIndex=0;
    wrapper.setAttribute('role','region');
    wrapper.setAttribute('aria-label','Scrollable records');
  });
});
