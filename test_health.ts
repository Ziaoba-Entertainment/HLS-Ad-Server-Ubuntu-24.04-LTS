async function test() {
  try {
    const res = await fetch('http://localhost:8083/health');
    console.log('Status:', res.status);
    console.log('Body:', await res.text());
  } catch (e) {
    console.error('Error:', e);
  }
}
test();
