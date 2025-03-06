const fp = require('fastify-plugin');

const customAsyncHandlers = {};

module.exports = fp(async function (fastify, opts) {

    const salesforceSdk= await import('@heroku/salesforce-sdk-nodejs');

    // Decorate request w/ 'salesforce' object providing hydrated Salesforce SDK APIs.
    fastify.decorateRequest('salesforce', null);

    /**
     * Salesforce PreHandler to enrich requests.  Requests made from
     * External Services contain additional context about the request
     * and invoking org.  Context is used to hydrated Salesforce SDK APIs.
     */
    const salesforcePreHandler = async (request, reply) => {
        request.sdk = salesforceSdk.init();

        const routeOptions = request.routeOptions;
        const hasSalesforceConfig = routeOptions.config && routeOptions.config.salesforce
        if (!(hasSalesforceConfig && routeOptions.config.salesforce.parseRequest === false)) {
            // Enrich request with hydrated SDK APIs
            const parsedRequest = request.sdk.salesforce.parseRequest(request.headers, request.body, request.log);
            request.sdk = Object.assign(request.sdk, parsedRequest);
        }
    }

    /**
     * Handler for asynchronous APIs to respond to requests immediately and
     * then perform functions.  The API may interact with other APIs and/or
     * the invoking org via External Service callback APIs as defined in the
     * operation's OpenAPI spec.
     *
     * @param request
     * @param reply
     * @returns {Promise<void>}
     */
    const asyncHandler = async (request, reply)=> {
        request.log.info(`Async response for ${request.method} ${request.routeOptions.url}`);

        const customAsyncHandler = request.routeOptions.config.salesforce.async;
        if (typeof customAsyncHandler === 'function') {
            await customAsyncHandler(request, reply);
        } else {
            reply.code(201);
        }
    };

    /**
     * Apply Salesforce preHandlers to routes.
     *
     * {config: {salesforce: {parseRequest: false}}}
     * Parsing is specific to External Service requests that contain additional
     * request context.  Setting parseRequest:false will not parse the request
     * to hydrate Salesforce SDK APIs to request.sdk.
     * This is useful when the request is NOT made * from an External Service.
     *
     * {config: {salesforce: {async: true || customResponseHandlerFunction}}},
     * When routes are configured as async, true applies the standard 201 response
     * or, if a function is given, the custom handler is invoked to respond to
     * the request.
     */
    fastify.addHook('onRoute', (routeOptions) => {
        const hasSalesforceConfig = routeOptions.config && routeOptions.config.salesforce

        if (!routeOptions.preHandler) {
            routeOptions.preHandler = [salesforcePreHandler];
        } else if (Array.isArray(routeOptions.preHandler)) {
            routeOptions.preHandler.push(salesforcePreHandler);
        }

        if (hasSalesforceConfig && routeOptions.config.salesforce.async) {
            const customAsyncHandler = routeOptions.handler;
            routeOptions.handler = asyncHandler;
            customAsyncHandlers[`${routeOptions.method} ${routeOptions.routePath}`] = customAsyncHandler;
            fastify.addHook('onResponse', async (request, reply) => {
                const routeIdx = `${request.method} ${request.routeOptions.url}`;
                if (request.sdk && request.sdk.asyncComplete === true) {
                    request.log.info(`${routeIdx} is async complete`);
                    return;
                }

                const customAsyncHandler = customAsyncHandlers[`${routeIdx}`];
                if (customAsyncHandler) {
                    request.log.info(`Found async handle for route index ${routeIdx}`);
                    await customAsyncHandler(request, reply);
                    request.sdk.asyncComplete = true;
                    request.log.info(`Set async ${routeIdx} completes`);
                }
            });
        }
    });

    /**
     * Healthcheck endpoint.
     */
    fastify.get('/healthcheck', {config: {salesforce: {parseRequest: false}}}, async function (request, reply) {
        reply.status(200).send('OK');
    });
});