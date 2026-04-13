export default {
  async fetch(request, env) {
    const hostname = env.AIRFLOW_PUBLIC_HOSTNAME;
    if (!hostname) {
      return new Response(
        "AIRFLOW_PUBLIC_HOSTNAME is not configured in wrangler.toml",
        { status: 500 }
      );
    }

    const incoming = new URL(request.url);
    const target = new URL(`https://${hostname}`);
    target.pathname = incoming.pathname;
    target.search = incoming.search;

    // Keep method/body and headers while forwarding through the public tunnel host.
    const forwarded = new Request(target.toString(), request);
    return fetch(forwarded, { redirect: "follow" });
  },
};

