package com.dimajix.common

object ScalaReflection {
    val universe = scala.reflect.runtime.universe

    import universe._

    def getConstructorParameters(tpe: Type): Seq[(String, Type)] = {
        val dealiasedTpe = tpe.dealias
        val formalTypeArgs = dealiasedTpe.typeSymbol.asClass.typeParams
        val TypeRef(_, _, actualTypeArgs) = dealiasedTpe
        val params = constructParams(dealiasedTpe)
        // if there are type variables to fill in, do the substitution (SomeClass[T] -> SomeClass[Int])
        if (actualTypeArgs.nonEmpty) {
            params.map { p =>
                p.name.decodedName.toString ->
                    p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
            }
        } else {
            params.map { p =>
                p.name.decodedName.toString -> p.typeSignature
            }
        }
    }

    private  def constructParams(tpe: Type): Seq[Symbol] = {
        val constructorSymbol = tpe.member(termNames.CONSTRUCTOR)
        val params = if (constructorSymbol.isMethod) {
            constructorSymbol.asMethod.paramLists
        } else {
            // Find the primary constructor, and use its parameter ordering.
            val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
                s => s.isMethod && s.asMethod.isPrimaryConstructor)
            if (primaryConstructorSymbol.isEmpty) {
                sys.error("Internal error: Product object did not have a primary constructor.")
            } else {
                primaryConstructorSymbol.get.asMethod.paramLists
            }
        }
        params.flatten
    }
}
