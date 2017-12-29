package net.ndolgov.scalapbtest

import java.util.concurrent.{Executor, TimeUnit}

import io.grpc.stub.AbstractStub
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.ExecutionContext

trait GrpcClient {
  def stop(): Unit

  def createClient[A <: AbstractStub[A]](f: (ManagedChannel) => A) : A

  def executionContext() : ExecutionContext
}

private final class GrpcClientImpl(channel: ManagedChannel, ec: ExecutionContext) extends GrpcClient {
  override def createClient[A <: AbstractStub[A]](factory: (ManagedChannel) => A) : A = {
    factory.apply(channel)
  }

  override def stop(): Unit = channel.shutdown().awaitTermination(5000, TimeUnit.MILLISECONDS)

  override def executionContext(): ExecutionContext = ec
}

/** Create GRPC transport to a given "host:port" destination */
object GrpcClient {
  def apply(hostname: String, port: Int, executor: Executor) : GrpcClient = {
    new GrpcClientImpl(
      ManagedChannelBuilder.forAddress(hostname, port).usePlaintext(true).executor(executor).build(),
      ExecutionContext.fromExecutor(executor))
  }
}
