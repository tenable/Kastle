package com.tenable.library.kafkaclient.testhelpers

import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.TryValues
import org.scalatest.matchers.must.Matchers

trait IntegrationSpec
    extends Eventually
    with EitherValues
    with OptionValues
    with TryValues
    with Matchers

trait AsyncIntegrationSpec extends AsyncWordSpec with IntegrationSpec
trait SyncIntegrationSpec  extends AnyWordSpec with IntegrationSpec
