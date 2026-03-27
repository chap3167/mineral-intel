exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  const STRIPE_SECRET = process.env.STRIPE_SECRET_KEY;

  const PRICES = {
    basic: 'price_1TCXj01EkEHfEdsNAhKU3tg0',
    pro: 'price_1TCXkn1EkEHfEdsNeFYCQ4Cd',
    enterprise: 'price_1TCXli1EkEHfEdsNpWvK3sH6',
  };

  try {
    const { email, plan } = JSON.parse(event.body || '{}');

    if (!email) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Email is required' }) };
    }

    const priceId = PRICES[plan] || PRICES.pro;
    const origin = event.headers.origin || event.headers.referer || 'https://mineralsearch.io';
    const baseUrl = origin.replace(/\/$/, '');

    const params = new URLSearchParams();
    params.append('payment_method_types[]', 'card');
    params.append('mode', 'subscription');
    params.append('customer_email', email);
    params.append('line_items[0][price]', priceId);
    params.append('line_items[0][quantity]', '1');
    params.append('success_url', baseUrl + '?payment=success&plan=' + (plan || 'pro') + '&email=' + encodeURIComponent(email));
    params.append('cancel_url', baseUrl + '?payment=cancelled');

    const response = await fetch('https://api.stripe.com/v1/checkout/sessions', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + STRIPE_SECRET,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: params.toString(),
    });

    const session = await response.json();

    if (session.error) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: session.error.message }),
      };
    }

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: session.url }),
    };
  } catch (err) {
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: err.message }),
    };
  }
};
