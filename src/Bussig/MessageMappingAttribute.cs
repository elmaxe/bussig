namespace Bussig;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface)]
public class MessageMappingAttribute : Attribute
{
    public MessageUrn Urn { get; private set; }

    public MessageMappingAttribute(string urn)
    {
        Urn = new MessageUrn(urn);
    }
}
