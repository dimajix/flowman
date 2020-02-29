package com.dimajix.flowman;

class JacksonTest extends FlatSpec with Matchers {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)

    import JacksonTest._
    "The BackReference" should "be filled out" in {
       val yaml =
           """
             |children:
             |   - name: lala
           """.stripMargin

        val data = mapper.readValue(yaml, classOf[SequenceContainer])

        data.children.size should be (1)
        data.children(0) should not be (null)
        data.children(0).name should be ("lala")
        data.children(0).parent should be (null)
    }

    "Case classes" should "be serialized with default values" in {
        val yaml =
            """
              |value: lala
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[CaseClassWithDefaults])
        //data.key should be ("key")
        data.value should be ("lala")
    }

    "Optional values" should "be supported" in {
        val yaml =
            """
              |key: some_key
              |val: some_value
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("some_key")
        data.value should be (Some("some_value"))
    }

    it should "support missing values" in {
        val yaml =
            """
              |key: some_key
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("some_key")
        data.value should be (null)
    }

    it should "support null values" in {
        val yaml =
            """
              |key: null
              |val: null
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be (null)
        data.value should be (None)
    }

    it should "support null strings" in {
        val yaml =
            """
              |key: "null"
              |val: "null"
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("null")
        data.value should be (Some("null"))
    }
}
