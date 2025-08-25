package com.example

import org.apache.pekko.actor.typed.ActorRef

trait RateLimiter {
  def allow(key: String, replyTo: ActorRef[Boolean]): Unit
}