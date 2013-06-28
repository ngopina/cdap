/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

/**
 *  Interface that needs to be implemented for handling HTTP methods. init and destroy methods can be used to manage
 *  the lifecycle of the object. The framework will call init and destroy during startup and shutdown respectively.
 *  No handles will be called before init method of the class is called or after the destroy method is called.
 *  The handlers should be annotated with Jax-RS annotations to handle appropriate path and HTTP Methods.
 *  Note: Only the annotations in the given handler object will be inspected and be available for routing. The
 *  annotations from the base class (if extended) will not be applied to the given handler object.
 *
 *  Example:
 *  public class ApiHandler implements HttpHandler{
 *    @Override
 *    public void init(){
 *      //Do some startup time stuff
 *    }
 *    @Override
 *    public void destroy(){
 *     //Do some shutdown stuff
 *    }
 *
 *    @Path("/common/v1/widgets")
 *    @GET
 *    public void handleGet(HttpRequest request, HttpResponder responder){
 *      //Handle Http request
 *    }
 *  }
 *
 */
public interface HttpHandler {

  /**
   * init method will be called before the netty pipeline is setup. Any initialization operation can be performed
   * in this method.
   */
  public void init();

  /**
   * destroy method will be called before shutdown. Any teardown task can be performed in this method.
   */
  public void destroy();
}
