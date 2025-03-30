package com.xiaoleilu.loServer.action.stub;

import com.xiaoleilu.loServer.action.Action;
import com.xiaoleilu.loServer.annotation.HttpMethod;
import com.xiaoleilu.loServer.annotation.Route;
import com.xiaoleilu.loServer.handler.Request;
import com.xiaoleilu.loServer.handler.Response;

/**
 * http://wildfirechat.net:443/route
 * @author nisus
 * @since 2025/3/24 22:48
 */
// @Route("/route")
// @HttpMethod("POST")
public class RouteAction extends Action {
    @Override
    public boolean action(Request request, Response response) {
        System.out.println(request);
        System.out.println(response);
        return true;
    }
}
