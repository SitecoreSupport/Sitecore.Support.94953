
using Sitecore.Data;
using Sitecore.Data.Managers;
using Sitecore.Diagnostics;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.StringExtensions;

namespace Sitecore.Support.ContentSearch
{
  public static class IIndexableExtension
  {
    public static IIndexableDataField GetFieldByKey(this IIndexable indexable, object fieldKey)
    {
      if (indexable == null || fieldKey == null)
      {
        return null;
      }

      // Is Sitecore Item Related?
      var itemIndexable = indexable as SitecoreIndexableItem;

      if (itemIndexable == null)
      {
        // No, default logic:
        return indexable.GetFieldById(fieldKey);
      }

      ID fieldId;

      if (ID.TryParse(fieldKey, out fieldId))
      {
        if (itemIndexable.OwnsField(fieldId))
        {
          return itemIndexable.GetFieldById(fieldId);
        }

        return null;
      }

      var fieldName = fieldKey as string;

      if (fieldName != null)
      {
        return indexable.GetFieldByName(fieldName);
      }

      return null;
    }

    private static bool OwnsField([NotNull] this SitecoreIndexableItem indexable, [NotNull] ID fieldId)
    {
      Assert.ArgumentNotNull(indexable, nameof(indexable));
      Assert.ArgumentNotNull(fieldId, nameof(fieldId));

      var item = indexable.Item;

      if (item == null)
      {
        CrawlingLog.Log.Warn("SUPPORT SitecoreIndexableItem '{0}' doesn't contain inner Item."
          .FormatWith(indexable.UniqueId)
        );

        return false;
      }

      return TemplateManager.IsFieldPartOfTemplate(fieldId, item);
    }
  }
}